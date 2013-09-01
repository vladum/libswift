/*
 *  zerostate.cpp
 *  manager for starting on-demand transfers that serve content and hashes
 *  directly from disk (so little state in memory). Requires content (named
 *  as roothash-in-hex), hashes (roothash-in-hex.mhash file) and checkpoint
 *  (roothash-in-hex.mbinmap) to be present on disk.
 *
 *  Note: currently zero-state implementations do not respond to PEX_REQ,
 *  there is no tracker functionality.
 *
 *  Created by Arno Bakker
 *  Copyright 2009-2016 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */
#include <cstdio>

#include "swift.h"
#include "compat.h"

using namespace swift;


ZeroState * ZeroState::__singleton = NULL;

#define CLEANUP_INTERVAL			30	// seconds

ZeroState::ZeroState() : contentdir_("."), connect_timeout_(TINT_NEVER)
{
    if (__singleton == NULL)
    {
        __singleton = this;
    }

    //fprintf(stderr,"ZeroState: registering clean up\n");
    evtimer_assign(&evclean_,Channel::evbase,&ZeroState::LibeventCleanCallback,this);
    evtimer_add(&evclean_,tint2tv(CLEANUP_INTERVAL*TINT_SEC));
}


ZeroState::~ZeroState()
{
    //fprintf(stderr,"ZeroState: deconstructor\n");

    // Arno, 2012-02-06: Cancel cleanup timer, otherwise chaos!
    evtimer_del(&evclean_);
}


void ZeroState::LibeventCleanCallback(int fd, short event, void *arg)
{
    //fprintf(stderr,"zero clean: enter\n");

    // Arno, 2012-02-24: Why-oh-why, update NOW
    Channel::Time();

    ZeroState *zs = (ZeroState *)arg;
    if (zs == NULL)
        return;

    // See which zero state FileTransfers have no clients
    tdlist_t tds = swift::GetTransferDescriptors();
    tdlist_t::iterator iter;
    tdlist_t delset;
    for (iter = tds.begin(); iter != tds.end(); iter++)
    {
        int td = *iter;

        if (!swift::IsZeroState(td))
            continue;

	ContentTransfer *ct = swift::GetActivatedTransfer(td);
	if (ct == NULL)
	{
	    // Not activated, so remove in any case
	    delset.push_back(td);
	    continue;
	}

    	// Arno, 2012-09-20: Work with copy of list, as "delete c" edits list.
    	channels_t copychans(*ct->GetChannels());
	if (copychans.size() == 0)
	    delset.push_back(td);
	else if (zs->connect_timeout_ != TINT_NEVER)
	{
	    // Garbage collect really slow connections, essential on Mac.
	    dprintf("%s zero clean %s has %lu peers\n",tintstr(),ct->swarm_id().hex().c_str(), ct->GetChannels()->size() );
	    channels_t::iterator iter2;
	    for (iter2=copychans.begin(); iter2!=copychans.end(); iter2++) {
		Channel *c = *iter2;
		if (c != NULL)
		{
		    //fprintf(stderr,"%s F%u zero clean %s opentime %lld connect %lld\n",tintstr(),ft->fd(), c->peer().str().c_str(), (NOW-c->GetOpenTime()), zs->connect_timeout_ );
		    // Garbage collect copychans when open for long and slow upload
		    if ((NOW-c->GetOpenTime()) > zs->connect_timeout_)
		    {
			//fprintf(stderr,"%s F%u zero clean %s opentime %lld ulspeed %lf\n",tintstr(),ft->fd(), c->peer().str().c_str(), (NOW-c->GetOpenTime())/TINT_SEC, ft->GetCurrentSpeed(DDIR_UPLOAD) );
			fprintf(stderr,"%s F%u zero clean %s close slow channel\n",tintstr(),td, c->peer().str() .c_str());
			dprintf("%s F%u zero clean %s close slow channel\n",tintstr(),td, c->peer().str().c_str() );
			c->Close(CLOSE_SEND_IF_ESTABLISHED);
			delete c;
		    }
		}
	    }
	    if (ct->GetChannels()->size() == 0)
	    {
		// Ain't go no clients left, cleanup transfer.
		delset.push_back(td);
	    }
	}
    }


    // Delete 0-state FileTransfers sans peers
    for (iter=delset.begin(); iter!=delset.end(); iter++)
    {
	int td = *iter;
	dprintf("%s hash %s zero clean close\n",tintstr(),SwarmID(td).hex().c_str() );
	//fprintf(stderr,"%s F%u zero clean close\n",tintstr(),td );
        swift::Close(td);
    }

    // Reschedule cleanup
    evtimer_add(&(zs->evclean_),tint2tv(CLEANUP_INTERVAL*TINT_SEC));
}



ZeroState * ZeroState::GetInstance()
{
    //fprintf(stderr,"ZeroState::GetInstance: %p\n", Channel::evbase );
    if (__singleton == NULL)
    {
        new ZeroState();
    }
    return __singleton;
}


void ZeroState::SetContentDir(std::string contentdir)
{
    contentdir_ = contentdir;
}

void ZeroState::SetConnectTimeout(tint timeout)
{
    //fprintf(stderr,"ZeroState: SetConnectTimeout: %lld\n", timeout/TINT_SEC );
    connect_timeout_ = timeout;
}

int ZeroState::Find(const Sha1Hash &root_hash)
{
    //fprintf(stderr,"swift: zero: Got request for %s\n",root_hash.hex().c_str() );

    //std::string file_name = "content.avi";
    std::string file_name = contentdir_+FILE_SEP+root_hash.hex();
    uint32_t chunk_size=SWIFT_DEFAULT_CHUNK_SIZE;

    dprintf("%s #0 zero find %s from %s\n",tintstr(),file_name.c_str(), getcwd_utf8().c_str() );

    std::string reqfilename = file_name;
    int ret = file_exists_utf8(reqfilename);
    if (ret < 0 || ret == 0 || ret == 2)
        return -1;
    reqfilename = file_name+".mbinmap";
    ret = file_exists_utf8(reqfilename);
    if (ret < 0 || ret == 0 || ret == 2)
        return -1;
    FILE *f = fopen(reqfilename.c_str(), "r");
    if (!f)
        return -1;
    fscanf(f, "version %*i\nroot hash %*s\nchunk size %u\n", &chunk_size);
    reqfilename = file_name+".mhash";
    ret = file_exists_utf8(reqfilename);
    if (ret < 0 || ret == 0 || ret == 2)
        return -1;

    // Open as ZeroState
    return swift::Open(file_name, root_hash, Address(), false, true, true, true, chunk_size);
}


void Channel::OnDataZeroState(struct evbuffer *evb)
{
    dprintf("%s #%u zero -data, don't need it, am a seeder\n",tintstr(),id_);
}

void Channel::OnHaveZeroState(struct evbuffer *evb)
{
    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    // Forget about it, i.e.. don't build peer binmap.
}

void Channel::OnHashZeroState(struct evbuffer *evb)
{
    dprintf("%s #%u zero -hash, don't need it, am a seeder\n",tintstr(),id_);
}

void Channel::OnPexAddZeroState(struct evbuffer *evb, int family)
{
    evbuffer_remove_pexaddr(evb, family);
    // Forget about it
}

void Channel::OnPexAddCertZeroState(struct evbuffer *evb)
{
    uint16_t size = evbuffer_remove_16be(evb);
    if (size > PEX_RES_MAX_CERT_SIZE || evbuffer_get_length(evb) < size) {
	dprintf("%s #%u ?pex cert too big\n",tintstr(),id_);
	return;
    }
    //swarmidbytes = evbuffer_pullup(evb,size);
    evbuffer_drain(evb, size);
    // Forget about it
}


void Channel::OnPexReqZeroState(struct evbuffer *evb)
{
    // Ignore it
}

