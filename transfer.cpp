/*
 *  transfer.cpp
 *  some transfer-scope code
 *
 *  Created by Victor Grishchenko on 10/6/09.
 *  Copyright 2009-2012 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */
#include <errno.h>
#include <string>
#include <sstream>
#include <vector>
#include <cfloat>
#include "swift.h"

#include "ext/seq_picker.cpp" // FIXME FIXME FIXME FIXME
#include "ext/vod_picker.cpp"

using namespace swift;

std::vector<FileTransfer*> FileTransfer::files(20);
bool FileTransfer::subscribe_channel_close = false;
swevent_list_t FileTransfer::subscribe_event_q;
double FileTransfer::global_max_speed_[] = {DBL_MAX, DBL_MAX};

#define BINHASHSIZE (sizeof(bin64_t)+sizeof(Sha1Hash))


#define TRACKER_RETRY_INTERVAL_START	(5*TINT_SEC)
#define TRACKER_RETRY_INTERVAL_EXP		1.1				// exponent used to increase INTERVAL_START
#define TRACKER_RETRY_INTERVAL_MAX		(1800*TINT_SEC) // 30 minutes

// FIXME: separate Bootstrap() and Download(), then Size(), Progress(), SeqProgress()

FileTransfer::FileTransfer(std::string filename, const Sha1Hash& root_hash, std::string metadir, bool force_check_diskvshash, bool check_netwvshash, uint32_t chunk_size, bool zerostate) :
	Operational(), cb_installed(0), mychannels_(),
    speedzerocount_(0), tracker_(), tracker_retry_interval_(TRACKER_RETRY_INTERVAL_START),
    tracker_retry_time_(NOW), fd_(files.size()+1), zerostate_(zerostate)
{
    if (files.size()<fd()+1)
        files.resize(fd()+1);
    files[fd()] = this;

    std::vector<std::string> p = filename2storagefns(filename,root_hash,metadir);
    // final filename, is destdir+roothash if orig filename was dir
    filename = p[0];
    // dir where data is saved, derived from filename
    std::string destdir = p[1];
    // full path for metadata files, append .m* etc for complete filename
    std::string metaprefix = p[2];

    /*fprintf(stderr,"FileTransfer: filename %s\n", filename.c_str() );
    fprintf(stderr,"FileTransfer: destdir  %s\n", destdir.c_str() );
    fprintf(stderr,"FileTransfer: metapref %s\n", metaprefix.c_str() );*/

    std::string hash_filename;
    hash_filename.assign(metaprefix);
    hash_filename.append(".mhash");

    std::string binmap_filename;
    binmap_filename.assign(metaprefix);
    binmap_filename.append(".mbinmap");

    // Arno, 2013-03-06: Try to open content read-only when seeding to avoid certain locking problems on
    // Win32 when opening the content with a different application in parallel
    uint64_t complete = 0;
    if (!zerostate_)
    {
	MmapHashTree *ht = new MmapHashTree(true,binmap_filename);
	if (root_hash == ht->root_hash())
	    complete = ht->complete();
	delete ht;
    }

	// MULTIFILE
    storage_ = new Storage(filename,destdir,fd(),complete);

    if (!zerostate_)
    {
	    hashtree_ = (HashTree *)new MmapHashTree(storage_,root_hash,chunk_size,hash_filename,force_check_diskvshash,check_netwvshash,binmap_filename);

	    if (ENABLE_VOD_PIECEPICKER) {
		    // Ric: init availability
		    availability_ = new Availability();
		    // Ric: TODO assign picker based on input params...
		    picker_ = new VodPiecePicker(this);
	    }
	    else
		    picker_ = new SeqPiecePicker(this);
	    picker_->Randomize(rand()&63);
    }
    else
    {
	    // ZEROHASH
	    hashtree_ = (HashTree *)new ZeroHashTree(storage_,root_hash,chunk_size,hash_filename,binmap_filename);
    }

    init_time_ = Channel::Time();
    cur_speed_[DDIR_UPLOAD] = MovingAverageSpeed();
    cur_speed_[DDIR_DOWNLOAD] = MovingAverageSpeed();
    max_speed_[DDIR_UPLOAD] = global_max_speed_[DDIR_UPLOAD];
    max_speed_[DDIR_DOWNLOAD] = global_max_speed_[DDIR_DOWNLOAD];

    // Per-swarm stats
    raw_bytes_[DDIR_UPLOAD] = 0;
    raw_bytes_[DDIR_DOWNLOAD] = 0;
    bytes_[DDIR_UPLOAD] = 0;
    bytes_[DDIR_DOWNLOAD] = 0;

    // SAFECLOSE
    evtimer_assign(&evclean_,Channel::evbase,&FileTransfer::LibeventCleanCallback,this);
    evtimer_add(&evclean_,tint2tv(5*TINT_SEC));

    UpdateOperational();
}


// SAFECLOSE
void FileTransfer::LibeventCleanCallback(int fd, short event, void *arg)
{
	// Arno, 2012-02-24: Why-oh-why, update NOW
	Channel::Time();

	FileTransfer *ft = (FileTransfer *)arg;
	if (ft == NULL)
		return;

	// STL and MS and conditional delete from set not a happy place :-(
	channels_t	delset;
	channels_t::iterator iter;
	bool hasestablishedpeers=false;
	for (iter=ft->mychannels_.begin(); iter!=ft->mychannels_.end(); iter++)
	{
		Channel *c = *iter;
		if (c != NULL) {
			if (c->IsScheduled4Close())
				delset.push_back(c);

			if (c->is_established ()) {
				hasestablishedpeers = true;
				//fprintf(stderr,"%s peer %s\n", ft->hashtree()->root_hash().hex().c_str(), c->peer().str() );
			}
		}
	}
	for (iter=delset.begin(); iter!=delset.end(); iter++)
	{
		Channel *c = *iter;
		dprintf("%s #%u clean cb close\n",tintstr(),c->id());
		c->Close();
		delete c; // Does erase from transfer() list of channels
    }

	// Arno, 2012-02-24: Check for liveliness.
	ft->ReConnectToTrackerIfAllowed(hasestablishedpeers);

	// Reschedule cleanup
    evtimer_add(&(ft->evclean_),tint2tv(5*TINT_SEC));
}


void FileTransfer::ReConnectToTrackerIfAllowed(bool hasestablishedpeers)
{
	// If I'm not connected to any
	// peers, try to contact the tracker again.
	if (!hasestablishedpeers)
	{
		if (NOW > tracker_retry_time_)
		{
			ConnectToTracker();

			tracker_retry_interval_ *= TRACKER_RETRY_INTERVAL_EXP;
			if (tracker_retry_interval_ > TRACKER_RETRY_INTERVAL_MAX)
				tracker_retry_interval_ = TRACKER_RETRY_INTERVAL_MAX;
			tracker_retry_time_ = NOW + tracker_retry_interval_;
		}
	}
	else
	{
		tracker_retry_interval_ = TRACKER_RETRY_INTERVAL_START;
		tracker_retry_time_ = NOW + tracker_retry_interval_;
	}
}


void FileTransfer::ConnectToTracker()
{
	if (!IsOperational())
		return;

	Channel *c = NULL;
    if (tracker_ != Address())
    	c = new Channel(this,INVALID_SOCKET,tracker_);
    else if (Channel::tracker!=Address())
    	c = new Channel(this);
}


Channel * FileTransfer::FindChannel(const Address &addr, Channel *notc)
{
	channels_t::iterator iter;
	for (iter=mychannels_.begin(); iter!=mychannels_.end(); iter++)
	{
		Channel *c = *iter;
		if (c != NULL) {
			if (c != notc && (c->peer() == addr || c->recv_peer() == addr)) {
				return c;
			}
		}
	}
	return NULL;
}


void FileTransfer::UpdateOperational()
{
    if ((hashtree_ != NULL && !hashtree_->IsOperational()) || !storage_->IsOperational())
    	SetBroken();
}


void    Channel::CloseTransfer (FileTransfer* trans) {
    for(int i=0; i<Channel::channels.size(); i++)
        if (Channel::channels[i] && Channel::channels[i]->transfer_==trans)
        {
        	//fprintf(stderr,"Channel::CloseTransfer: delete #%i\n", Channel::channels[i]->id());
        	Channel::channels[i]->Close(); // ARNO
            delete Channel::channels[i];
        }
}


void swift::AddProgressCallback (int transfer,ProgressCallback cb,uint8_t agg) {

	//fprintf(stderr,"swift::AddProgressCallback: transfer %i\n", transfer );

    FileTransfer* trans = FileTransfer::file(transfer);
    if (!trans)
        return;

    //fprintf(stderr,"swift::AddProgressCallback: ft obj %p %p\n", trans, cb );

    trans->cb_agg[trans->cb_installed] = agg;
    trans->callbacks[trans->cb_installed] = cb;
    trans->cb_installed++;
}


void swift::ExternallyRetrieved (int transfer,bin_t piece) {
    FileTransfer* trans = FileTransfer::file(transfer);
    if (!trans)
        return;
    trans->ack_out()->set(piece); // that easy
}


void swift::RemoveProgressCallback (int transfer, ProgressCallback cb) {

	//fprintf(stderr,"swift::RemoveProgressCallback: transfer %i\n", transfer );

    FileTransfer* trans = FileTransfer::file(transfer);
    if (!trans)
        return;

    //fprintf(stderr,"swift::RemoveProgressCallback: transfer %i ft obj %p %p\n", transfer, trans, cb );

    for(int i=0; i<trans->cb_installed; i++)
        if (trans->callbacks[i]==cb)
            trans->callbacks[i]=trans->callbacks[--trans->cb_installed];

    for(int i=0; i<trans->cb_installed; i++)
    {
    	fprintf(stderr,"swift::RemoveProgressCallback: transfer %i remain %p\n", transfer, trans->callbacks[i] );
    }
}


FileTransfer::~FileTransfer ()
{
    Channel::CloseTransfer(this);
	delete hashtree_;
	delete storage_;
    files[fd()] = NULL;
	if (!IsZeroState())
	{
		delete picker_;
		delete availability_;
	}
  
    // Arno, 2012-02-06: Cancel cleanup timer, otherwise chaos!
    evtimer_del(&evclean_);
}


FileTransfer* FileTransfer::Find (const Sha1Hash& root_hash) {
    for(int i=0; i<files.size(); i++)
        if (files[i] && files[i]->root_hash()==root_hash)
            return files[i];
    return NULL;
}


int swift:: Find (Sha1Hash hash) {
    FileTransfer* t = FileTransfer::Find(hash);
    if (t)
        return t->fd();
    return -1;
}



bool FileTransfer::OnPexAddIn (const Address& addr) {

	//fprintf(stderr,"FileTransfer::OnPexAddIn: %s\n", addr.str() );
	// Arno: this brings safety, but prevents private swift installations.
	// TODO: detect public internet.
	//if (addr.is_private())
	//	return false;
    // Gertjan fix: PEX redo
    if (hs_in_.size()<SWIFT_MAX_CONNECTIONS)
    {
    	// Arno, 2012-02-27: Check if already connected to this peer.
		Channel *c = FindChannel(addr,NULL);
		if (c == NULL)
			new Channel(this,Channel::default_socket(),addr);
		else
			return false;
    }
    return true;
}

//Gertjan
int FileTransfer::RandomChannel (int own_id) {
    binqueue choose_from;
    int i;

    for (i = 0; i < (int) hs_in_.size(); i++) {
        if (hs_in_[i].toUInt() == own_id)
            continue;
        Channel *c = Channel::channel(hs_in_[i].toUInt());
        if (c == NULL || c->transfer().fd() != this->fd()) {
            /* Channel was closed or is not associated with this FileTransfer (anymore). */
            hs_in_[i] = hs_in_[0];
            hs_in_.pop_front();
            i--;
            continue;
        }
        if (!c->is_established())
            continue;
        choose_from.push_back(hs_in_[i]);
    }
    if (choose_from.size() == 0)
        return -1;

    return choose_from[rand() % choose_from.size()].toUInt();
}

void		FileTransfer::OnRecvData(int n)
{
	// Got n ~ 32K
	cur_speed_[DDIR_DOWNLOAD].AddPoint((uint64_t)n);
}

void		FileTransfer::OnSendData(int n)
{
	// Sent n ~ 1K
	cur_speed_[DDIR_UPLOAD].AddPoint((uint64_t)n);
}


void		FileTransfer::OnSendNoData()
{
	// AddPoint(0) everytime we don't AddData gives bad speed measurement
	// batch 32 such events into 1.
	speedzerocount_++;
	if (speedzerocount_ >= 32)
	{
		cur_speed_[DDIR_UPLOAD].AddPoint((uint64_t)0);
		speedzerocount_ = 0;
	}
}


double		FileTransfer::GetCurrentSpeed(data_direction_t ddir)
{
	return cur_speed_[ddir].GetSpeedNeutral();
}


void		FileTransfer::SetMaxSpeed(data_direction_t ddir, double m)
{
	max_speed_[ddir] = m;
	// Arno, 2012-01-04: Be optimistic, forget history.
	cur_speed_[ddir].Reset();
}

void        FileTransfer::SetGlobalMaxSpeed(data_direction_t ddir, double m)
{
    global_max_speed_[ddir] = m;
}


double		FileTransfer::GetMaxSpeed(data_direction_t ddir)
{
	return max_speed_[ddir];
}


uint32_t	FileTransfer::GetNumLeechers()
{
	uint32_t count = 0;
	channels_t::iterator iter;
    for (iter=mychannels_.begin(); iter!=mychannels_.end(); iter++)
    {
	    Channel *c = *iter;
	    if (c != NULL)
		    if (!c->IsComplete()) // incomplete?
			    count++;
    }
    return count;
}


uint32_t	FileTransfer::GetNumSeeders()
{
	uint32_t count = 0;
	channels_t::iterator iter;
    for (iter=mychannels_.begin(); iter!=mychannels_.end(); iter++)
    {
	    Channel *c = *iter;
	    if (c != NULL)
		    if (c->IsComplete()) // complete?
			    count++;
    }
    return count;
}


void FileTransfer::AddPeer(Address &peer)
{
	Channel *c = new Channel(this,INVALID_SOCKET,peer);
}


uint64_t FileTransfer::GetBytes(data_direction_t ddir)
{
    return bytes_[ddir];
}

uint64_t FileTransfer::GetRawBytes(data_direction_t ddir)
{
    return raw_bytes_[ddir];
}


void FileTransfer::AddBytes(data_direction_t ddir,uint32_t a)
{
    bytes_[ddir] += a;
}

void FileTransfer::AddRawBytes(data_direction_t ddir,uint32_t a)
{
    raw_bytes_[ddir] += a;
}



std::vector<std::string> swift::filename2storagefns(std::string filename,const Sha1Hash &root_hash,std::string metadir)
{
    std::string destdir;
    std::string metaprefix;
    int ret = file_exists_utf8(filename);
    if (ret == 2 && root_hash != Sha1Hash::ZERO) {
	    // Filename is a directory, download root_hash there
	    destdir = filename;
	    filename = destdir+FILE_SEP+root_hash.hex();
	    if (!metadir.compare(""))
		metaprefix = filename;
	    else
		metaprefix = metadir+root_hash.hex();
    } else {
	    destdir = dirname_utf8(filename);
	    if (!destdir.compare(""))
	    {
		// Filename without dir
		destdir = ".";
		// filename is basename
		if (!metadir.compare(""))
		    metaprefix = filename;
		else
		    metaprefix = metadir+FILE_SEP+filename;
	    }
	    else
	    {
		// Filename with directory
		std::string basename = basename_utf8(filename);
		if (!metadir.compare(""))
		    metaprefix = filename;
		else
		    metaprefix = metadir+basename;
	    }
    }
    std::vector<std::string> svec;
    svec.push_back(filename);
    svec.push_back(destdir);
    svec.push_back(metaprefix);
    return svec;
}
