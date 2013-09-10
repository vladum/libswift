/*
 *  cmdgw.cpp
 *  command gateway for controling swift engine via a TCP connection
 *
 *  Created by Arno Bakker
 *  Copyright 2010-2012 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */
#include <math.h>
#include <iostream>
#include <sstream>

#include "swift.h"
#include "compat.h"
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>


using namespace swift;

// Send PLAY after receiving N bytes
#define CMDGW_MAX_PREBUF_BYTES		(256*1024)

// Report swift download progress every 2^layer * chunksize bytes (so 0 = report every chunk)
#define CMDGW_FIRST_PROGRESS_BYTE_INTERVAL_AS_LAYER 	0

// Status of the swarm download
#define DLSTATUS_ALLOCATING_DISKSPACE 0
#define DLSTATUS_HASHCHECKING  2
#define DLSTATUS_DOWNLOADING  3
#define DLSTATUS_SEEDING 4
#define DLSTATUS_STOPPED_ON_ERROR  6

#define MAX_CMD_MESSAGE 1024

#define ERROR_NO_ERROR		0
#define ERROR_UNKNOWN_CMD	-1
#define ERROR_MISS_ARG		-2
#define ERROR_BAD_ARG		-3
#define ERROR_BAD_SWARM		-4

#define CMDGW_MAX_CLIENT 1024   // Arno: == maximum number of swarms per proc

struct cmd_gw_t {
    int      id;
    evutil_socket_t   cmdsock;
    int		 transfer; 			 // swift FD
    bool	moreinfo;		  // whether to report detailed stats (see SETMOREINFO cmd)
    tint 	startt;			  // ARNOSMPTODO: debug speed measurements, remove
    std::string mfspecname;	  // MULTIFILE
    uint64_t startoff;   // MULTIFILE: starting offset in content range of desired file
    uint64_t endoff;     // MULTIFILE: ending offset (careful, for an e.g. 100 byte interval this is 99)

} cmd_requests[CMDGW_MAX_CLIENT];


int cmd_gw_reqs_open = 0;
int cmd_gw_reqs_count = 0;
int cmd_gw_conns_open = 0;

struct evconnlistener *cmd_evlistener = NULL;
struct evbuffer *cmd_evbuffer = NULL; // Data received on cmd socket : WARNING: one for all cmd sockets

/*
 * SOCKTUNNEL
 * We added the ability for a process to tunnel data over swift's UDP socket.
 * The process should send TUNNELSEND commands over the CMD TCP socket and will
 * receive TUNNELRECV commands from swift, containing data received via UDP
 * on channel 0xffffffff.
 */
typedef enum {
	CMDGW_TUNNEL_SCAN4CRLF,
	CMDGW_TUNNEL_READTUNNEL
} cmdgw_tunnel_t;

cmdgw_tunnel_t cmd_tunnel_state=CMDGW_TUNNEL_SCAN4CRLF;
uint32_t	 cmd_tunnel_expect=0;
Address		 cmd_tunnel_dest_addr;
uint32_t 	 cmd_tunnel_dest_chanid;
evutil_socket_t   cmd_tunnel_sock=INVALID_SOCKET;

// HTTP gateway address for PLAY cmd
Address cmd_gw_httpaddr;
std::string cmd_gw_metadir;

bool cmd_gw_debug=true;

tint cmd_gw_last_open=0;


// Fwd defs
void CmdGwDataCameInCallback(struct bufferevent *bev, void *ctx);
bool CmdGwReadLine(evutil_socket_t cmdsock);
void CmdGwNewRequestCallback(evutil_socket_t cmdsock, char *line);
void CmdGwProcessData(evutil_socket_t cmdsock);


void CmdGwFreeRequest(cmd_gw_t* req)
{
    req->id = -1;
    req->cmdsock = -1;
    req->transfer = -1;
    req->moreinfo = false;
    req->startt = 0;
    req->mfspecname = "";
    req->startoff = -1;
    req->endoff = -1;
}


void CmdGwCloseConnection(evutil_socket_t sock)
{
	// Close cmd connection and stop all associated downloads.
	// Doesn't remove .mhash state or content

	if (cmd_gw_debug)
		fprintf(stderr,"CmdGwCloseConnection: ENTER %d\n", sock );

	bool scanning = true;
	while (scanning)
	{
		scanning = false;
	    for(int i=0; i<cmd_gw_reqs_open; i++)
	    {
	    	cmd_gw_t* req = &cmd_requests[i];
	        if (req->cmdsock==sock)
	        {
                dprintf("%s @%i stopping-on-close transfer %i\n",tintstr(),req->id,req->transfer);
                swift::Close(req->transfer);

                // Remove from list and reiterate over it
                CmdGwFreeRequest(req);
	        	*req = cmd_requests[--cmd_gw_reqs_open];
	        	scanning = true;
	        	break;
	        }
	    }
	}

	// Arno, 2012-07-06: Close
	swift::close_socket(sock);

	cmd_gw_conns_open--;

	// Arno, 2012-10-11: New policy Immediate shutdown on connection close,
	// see CmdGwUpdateDLStatesCallback()
	fprintf(stderr,"cmd: Shutting down on CMD connection close\n");
	event_base_loopexit(Channel::evbase, NULL);
}


cmd_gw_t* CmdGwFindRequestByTransfer (int transfer)
{
    for(int i=0; i<cmd_gw_reqs_open; i++)
        if (cmd_requests[i].transfer==transfer)
            return cmd_requests+i;
    return NULL;
}

cmd_gw_t* CmdGwFindRequestByRootHash(Sha1Hash &want_hash)
{
	FileTransfer *ft = NULL;
    for(int i=0; i<cmd_gw_reqs_open; i++) {
    	cmd_gw_t* req = &cmd_requests[i];
    	ft = FileTransfer::file(req->transfer);
		if (ft == NULL)
			continue;
    	Sha1Hash got_hash = ft->root_hash();
        if (want_hash == got_hash)
        	return req;
    }
    return NULL;
}


void CmdGwGotCHECKPOINT(Sha1Hash &want_hash)
{
    // Checkpoint the specified download
    if (cmd_gw_debug)
        fprintf(stderr,"cmd: GotCHECKPOINT: %s\n",want_hash.hex().c_str());

    cmd_gw_t* req = CmdGwFindRequestByRootHash(want_hash);
    if (req == NULL)
    	return;

    swift::Checkpoint(req->transfer);
}


void CmdGwGotREMOVE(Sha1Hash &want_hash, bool removestate, bool removecontent)
{
	// Remove the specified download
	if (cmd_gw_debug)
		fprintf(stderr,"cmd: GotREMOVE: %s %d %d\n",want_hash.hex().c_str(),removestate,removecontent);

	cmd_gw_t* req = CmdGwFindRequestByRootHash(want_hash);
	if (req == NULL)
	{
		if (cmd_gw_debug)
			fprintf(stderr,"cmd: GotREMOVE: %s not found, bad swarm?\n",want_hash.hex().c_str());
    	return;
	}
    FileTransfer *ft = FileTransfer::file(req->transfer);
    if (ft == NULL)
    	return;

	dprintf("%s @%i remove transfer %i\n",tintstr(),req->id,req->transfer);

	//MULTIFILE
	// Arno, 2012-05-23: Copy all filename to be deleted to a set. This info is lost after
	// swift::Close() and we need to call Close() to let the storage layer close the open files.
    // TODO: remove the dirs we created, if now empty.
	std::set<std::string>	delset;
	std::string contentfilename = ft->GetStorage()->GetOSPathName();

	// Delete content + .mhash from filesystem, if desired
	if (removecontent)
		delset.insert(contentfilename);

	if (removestate)
	{
		std::string mhashfilename = ft->hashtree()->get_hash_filename();
		delset.insert(mhashfilename);

		// Arno, 2012-01-10: .mbinmap gots to go too.
		std::string mbinmapfilename = ft->hashtree()->get_binmap_filename();;
		delset.insert(mbinmapfilename);
	}

	// MULTIFILE
	if (removecontent && ft->GetStorage()->IsReady())
	{
		storage_files_t::iterator iter;
		storage_files_t sfs = ft->GetStorage()->GetStorageFiles();
		for (iter = sfs.begin(); iter < sfs.end(); iter++)
		{
			StorageFile *sf = *iter;
			std::string cfn = sf->GetOSPathName();
			delset.insert(cfn);
		}
	}

	swift::Close(req->transfer);
	ft = NULL;
	// All ft info now invalid

	std::set<std::string>::iterator iter;
	for (iter=delset.begin(); iter!=delset.end(); iter++)
	{
		std::string filename = *iter;
		if (cmd_gw_debug)
			fprintf(stderr,"CmdGwREMOVE: removing %s\n", filename.c_str() );
		int ret = remove_utf8(filename);
		if (ret < 0)
		{
			if (cmd_gw_debug)
				print_error("Could not remove file");
		}
	}

	CmdGwFreeRequest(req);
	*req = cmd_requests[--cmd_gw_reqs_open];
}


void CmdGwGotMAXSPEED(Sha1Hash &want_hash, data_direction_t ddir, double speed)
{
	// Set maximum speed on the specified download
	//fprintf(stderr,"cmd: GotMAXSPEED: %s %d %lf\n",want_hash.hex().c_str(),ddir,speed);

    if (want_hash == Sha1Hash::ZERO) {
        // Set global for all future transfers.
        FileTransfer::SetGlobalMaxSpeed(ddir, speed);
        return;
    }

	cmd_gw_t* req = CmdGwFindRequestByRootHash(want_hash);
	if (req == NULL)
    	return;
    FileTransfer *ft = FileTransfer::file(req->transfer);

    // Arno, 2012-05-25: SetMaxSpeed resets the current speed history, so
    // be careful here.
    double curmax = ft->GetMaxSpeed(ddir);
    if (curmax != speed)
    {
    	if (cmd_gw_debug)
    		fprintf(stderr,"cmd: CmdGwGotMAXSPEED: %s was %lf want %lf, setting\n", want_hash.hex().c_str(), curmax, speed );
    	ft->SetMaxSpeed(ddir,speed);
    }
}


void CmdGwGotSETMOREINFO(Sha1Hash &want_hash, bool enable)
{
	cmd_gw_t* req = CmdGwFindRequestByRootHash(want_hash);
	if (req == NULL)
    	return;
	req->moreinfo = enable;
}

void CmdGwGotRECIPROCITY(char *message)
{
    // Pass message to the reciprocity policy object.
    Channel::reciprocity_policy()->ExternalCmd(message);
}

void CmdGwGotPEERADDR(Sha1Hash &want_hash, Address &peer)
{
	cmd_gw_t* req = CmdGwFindRequestByRootHash(want_hash);
	if (req == NULL)
    	return;
	FileTransfer *ft = FileTransfer::file(req->transfer);
	if (ft == NULL)
		return;

	ft->AddPeer(peer);
}



void CmdGwSendINFOHashChecking(evutil_socket_t cmdsock, Sha1Hash root_hash)
{
	// Send INFO DLSTATUS_HASHCHECKING message.

    char cmd[MAX_CMD_MESSAGE];
    sprintf(cmd,"INFO %s %d %lli/%lli %lf %lf %u %u %llu %llu\r\n",root_hash.hex().c_str(),DLSTATUS_HASHCHECKING,(uint64_t)0,(uint64_t)0,(double)0.0,(double)0.0,0,0,(uint64_t)0,(uint64_t)0);

    //fprintf(stderr,"cmd: SendINFO: %s", cmd);
    send(cmdsock,cmd,strlen(cmd),0);
}


void CmdGwSendINFO(cmd_gw_t* req, int dlstatus)
{
	// Send INFO message.
	if (cmd_gw_debug)
		fprintf(stderr,"cmd: SendINFO: F%d initdlstatus %d\n", req->transfer, dlstatus );

	FileTransfer *ft = FileTransfer::file(req->transfer);
	if (ft == NULL)
		// Download was removed or closed somehow.
		return;

    Sha1Hash root_hash = ft->root_hash();

    char cmd[MAX_CMD_MESSAGE];
    uint64_t size = swift::Size(req->transfer);
    uint64_t complete = swift::Complete(req->transfer);
    if (size > 0 && size == complete)
    	dlstatus = DLSTATUS_SEEDING;
    if (!ft->IsOperational())
    	dlstatus = DLSTATUS_STOPPED_ON_ERROR;

    uint32_t numleech = ft->GetNumLeechers();
    uint32_t numseeds = ft->GetNumSeeders();

    double dlspeed = ft->GetCurrentSpeed(DDIR_DOWNLOAD);
    double ulspeed = ft->GetCurrentSpeed(DDIR_UPLOAD);
    sprintf(cmd,"INFO %s %d %lli/%lli %lf %lf %u %u %llu %llu\r\n",root_hash.hex().c_str(),dlstatus,complete,size,dlspeed,ulspeed,numleech,numseeds,ft->GetBytes(DDIR_DOWNLOAD),ft->GetBytes(DDIR_UPLOAD));

    send(req->cmdsock,cmd,strlen(cmd),0);

    // MORESTATS
    if (req->moreinfo) {
    	// Send detailed ul/dl stats in JSON format.

    	std::ostringstream oss;
    	oss.setf(std::ios::fixed,std::ios::floatfield);
    	oss.precision(5);
    	channels_t::iterator iter;
    	channels_t peerchans = ft->GetChannels();

        oss << "MOREINFO" << " " << root_hash.hex() << " ";

        double tss = (double)Channel::Time() / 1000000.0L;
        oss << "{\"timestamp\":\"" << tss << "\", ";
        oss << "\"channels\":";
        oss << "[";
        for (iter=peerchans.begin(); iter!=peerchans.end(); iter++) {
    		Channel *c = *iter;
    		if (c != NULL) {
    			if (iter!=peerchans.begin())
    				oss << ", ";
    			oss << "{";
    			oss << "\"ip\": \"" << c->peer().ipv4str() << "\", ";
    			oss << "\"port\": " << c->peer().port() << ", ";
    			oss << "\"raw_bytes_up\": " << c->raw_bytes_up() << ", ";
    			oss << "\"raw_bytes_down\": " << c->raw_bytes_down() << ", ";
    			oss << "\"bytes_up\": " << c->bytes_up() << ", ";
    			oss << "\"bytes_down\": " << c->bytes_down() << " ";
    			oss << "}";
    		}
        }
        oss << "], ";
        oss << "\"raw_bytes_up\": " << Channel::global_raw_bytes_up << ", ";
        oss << "\"raw_bytes_down\": " << Channel::global_raw_bytes_down << ", ";
        oss << "\"bytes_up\": " << Channel::global_bytes_up << ", ";
        oss << "\"bytes_down\": " << Channel::global_bytes_down << " ";
        oss << "}";

        oss << "\r\n";

        std::stringbuf *pbuf=oss.rdbuf();
        size_t slen = strlen(pbuf->str().c_str());
        send(req->cmdsock,pbuf->str().c_str(),slen,0);
    }
}


void CmdGwSendPLAY(cmd_gw_t *req)
{
	// Send PLAY message to user
	if (cmd_gw_debug)
		fprintf(stderr,"cmd: SendPLAY: %d\n", req->transfer );

    Sha1Hash root_hash = FileTransfer::file(req->transfer)->root_hash();

    char cmd[MAX_CMD_MESSAGE];
    // Slightly diff format: roothash as ID after CMD
    if (req->mfspecname == "")
    	sprintf(cmd,"PLAY %s http://%s/%s\r\n",root_hash.hex().c_str(),cmd_gw_httpaddr.str(),root_hash.hex().c_str());
    else
    	sprintf(cmd,"PLAY %s http://%s/%s/%s\r\n",root_hash.hex().c_str(),cmd_gw_httpaddr.str(),root_hash.hex().c_str(),req->mfspecname.c_str());

    if (cmd_gw_debug)
        fprintf(stderr,"cmd: SendPlay: %s", cmd);

    send(req->cmdsock,cmd,strlen(cmd),0);
}


void CmdGwSendERRORBySocket(evutil_socket_t cmdsock, std::string msg, const Sha1Hash& roothash=Sha1Hash::ZERO)
{
	std::string cmd = "ERROR ";
	cmd += roothash.hex();
	cmd += " ";
	cmd += msg;
	cmd += "\r\n";

	if (cmd_gw_debug)
		fprintf(stderr,"cmd: SendERROR: %s\n", cmd.c_str() );

	char *wire = strdup(cmd.c_str());
	send(cmdsock,wire,strlen(wire),0);
	free(wire);
}


void CmdGwSwiftPrebufferProgressCallback (int transfer, bin_t bin)
{
	//
	// Subsequent bytes of content downloaded
	//
	//if (cmd_gw_debug)
	//	fprintf(stderr,"cmd: SwiftPrebuffProgress: %d\n", transfer );

	cmd_gw_t* req = CmdGwFindRequestByTransfer(transfer);
	if (req == NULL)
		return;

#ifdef WIN32
	int64_t wantsize = min(req->endoff+1-req->startoff,CMDGW_MAX_PREBUF_BYTES);
#else
        int64_t wantsize = std::min(req->endoff+1-req->startoff,(uint64_t)CMDGW_MAX_PREBUF_BYTES);
#endif

	//if (cmd_gw_debug)
	//	fprintf(stderr,"cmd: SwiftPrebuffProgress: want %lld got %lld\n", swift::SeqComplete(req->transfer,req->startoff), wantsize );


	if (swift::SeqComplete(req->transfer,req->startoff) >= wantsize)
	{
		// First CMDGW_MAX_PREBUF_BYTES bytes received via swift,
		// tell user to PLAY
		// ARNOSMPTODO: bitrate-dependent prebuffering?
		if (cmd_gw_debug)
			fprintf(stderr,"cmd: SwiftPrebufferProgress: Prebuf done %d\n", transfer );

		swift::RemoveProgressCallback(transfer,&CmdGwSwiftPrebufferProgressCallback);

		CmdGwSendPLAY(req);
	}
	// wait for prebuffer
}


/*
 * For single file content, install a new callback that checks whether
 * we have enough data prebuffered. For multifile, wait till the first chunks
 * containing the multi-file spec have been loaded, then set download pointer
 * to the desired file (via swift::Seek) and then wait till enough data is
 * prebuffered (via CmdGwSwiftPrebufferingProcessCallback).
 */

void CmdGwSwiftFirstProgressCallback (int transfer, bin_t bin)
{
	//
	// First bytes of content downloaded (first in absolute sense)
	//
	if (cmd_gw_debug)
		fprintf(stderr,"cmd: SwiftFirstProgress: %d\n", transfer );

	cmd_gw_t* req = CmdGwFindRequestByTransfer(transfer);
	if (req == NULL)
		return;

	FileTransfer *ft = FileTransfer::file(req->transfer);
	if (ft == NULL) {
		CmdGwSendERRORBySocket(req->cmdsock,"Unknown transfer?!",ft->root_hash());
    	return;
	}
	if (!ft->GetStorage()->IsReady()) {
		// Wait until (multi-file) storage is ready
		return;
	}

	swift::RemoveProgressCallback(transfer,&CmdGwSwiftFirstProgressCallback);

	if (req->mfspecname == "")
	{
		// Single file
		req->startoff = 0;
		req->endoff = swift::Size(req->transfer)-1;
		CmdGwSwiftPrebufferProgressCallback(req->transfer,bin_t(0,0)); // in case file on disk
		swift::AddProgressCallback(transfer,&CmdGwSwiftPrebufferProgressCallback,CMDGW_FIRST_PROGRESS_BYTE_INTERVAL_AS_LAYER);
	}
	else
	{
		// MULTIFILE
		// Have spec, seek to wanted file
		storage_files_t sfs = ft->GetStorage()->GetStorageFiles();
		storage_files_t::iterator iter;
		bool found = false;
		for (iter = sfs.begin(); iter < sfs.end(); iter++)
		{
			StorageFile *sf = *iter;
			if (sf->GetSpecPathName() == req->mfspecname)
			{
				if (cmd_gw_debug)
					fprintf(stderr,"cmd: SwiftFirstProgress: Seeking to multifile %s for %d\n", req->mfspecname.c_str(), transfer );

				int ret = swift::Seek(req->transfer,sf->GetStart(),SEEK_SET);
				if (ret < 0)
				{
					CmdGwSendERRORBySocket(req->cmdsock,"Error seeking to file in multi-file content.",ft->root_hash());
					return;
				}
				found = true;
				req->startoff = sf->GetStart();
				req->endoff = sf->GetEnd();
				CmdGwSwiftPrebufferProgressCallback(req->transfer,bin_t(0,0)); // in case file on disk
				swift::AddProgressCallback(transfer,&CmdGwSwiftPrebufferProgressCallback,CMDGW_FIRST_PROGRESS_BYTE_INTERVAL_AS_LAYER);
				break;
			}
		}
		if (!found) {

			if (cmd_gw_debug)
				fprintf(stderr,"cmd: SwiftFirstProgress: Error file not found %d\n", transfer );

			CmdGwSendERRORBySocket(req->cmdsock,"Individual file not found in multi-file content.",ft->root_hash());
			return;
		}
	}
}


void CmdGwSwiftErrorCallback (evutil_socket_t cmdsock)
{
	// Error on swift socket callback

	const char *response = "ERROR Swift Engine Problem\r\n";
	send(cmdsock,response,strlen(response),0);

	//swift::close_socket(sock);
}

void CmdGwSwiftAllocatingDiskspaceCallback (int transfer, bin_t bin)
{
	if (cmd_gw_debug)
		fprintf(stderr,"cmd: CmdGwSwiftAllocatingDiskspaceCallback: ENTER %d\n", transfer );

	// Called before swift starts reserving diskspace.
	cmd_gw_t* req = CmdGwFindRequestByTransfer(transfer);
	if (req == NULL)
		return;

	CmdGwSendINFO(req,DLSTATUS_ALLOCATING_DISKSPACE);
}



void CmdGwUpdateDLStateCallback(cmd_gw_t* req)
{
	// Periodic callback, tell user INFO
	CmdGwSendINFO(req,DLSTATUS_DOWNLOADING);

	// Update speed measurements such that they decrease when DL/UL stops
	FileTransfer *ft = FileTransfer::file(req->transfer);
	if (ft == NULL) // Concurrency between ERROR_BAD_SWARM and this periodic callback
		return;
	ft->OnRecvData(0);
	ft->OnSendData(0);

	if (false)
	{
		// DEBUG download speed rate limit
		double dlspeed = ft->GetCurrentSpeed(DDIR_DOWNLOAD);
#ifdef WIN32
		double dt = max(0.000001,(double)(usec_time() - req->startt)/TINT_SEC);
#else
		double dt = std::max(0.000001,(double)(usec_time() - req->startt)/TINT_SEC);
#endif
		double exspeed = (double)(swift::Complete(req->transfer)) / dt;
		fprintf(stderr,"cmd: UpdateDLStateCallback: SPEED %lf == %lf\n", dlspeed, exspeed );
	}
}


int icount=0;

void CmdGwUpdateDLStatesCallback()
{
    // Called by swift main approximately every second
    // Loop over all swarms
    evutil_socket_t cmdsock=-1;
    for(int i=0; i<cmd_gw_reqs_open; i++)
    {
    	cmd_gw_t* req = &cmd_requests[i];
    	CmdGwUpdateDLStateCallback(req);
    	cmdsock=req->cmdsock;
    }

    // Arno, 2012-05-24: Autoclose if CMD *connection* not *re*established soon
    // Arno, 2012-10-11: Replaced with shutdown-on-connection error, such that
    // external client can be fully responsible for restarting and process mgmt.
    /*if (cmd_gw_conns_open == 0)
    {
    	if (cmd_gw_last_open > 0)
    	{
    		tint diff = NOW - cmd_gw_last_open;
    		//fprintf(stderr,"cmd: time since last conn diff %lld\n", diff );
    		if (diff > 10*TINT_SEC)
    		{
    			fprintf(stderr,"cmd: No CMD connection since X sec, shutting down\n");
    			event_base_loopexit(Channel::evbase, NULL);
    		}
    	}
    }
    else
    	cmd_gw_last_open = NOW;
    */

    if (cmdsock != -1 && FileTransfer::subscribe_channel_close)
    {
    	std::ostringstream oss;

	swevent_list_t::iterator iter;
	for (iter = FileTransfer::subscribe_event_q.begin(); iter != FileTransfer::subscribe_event_q.end(); iter++)
	{
	    CloseEvent ce = *iter;

	    oss << "CLOSE_EVENT" << " " << ce.swarmid().hex() << " ";
	    oss << ce.peeraddr().str() << " ";
	    oss << ce.raw_bytes(DDIR_UPLOAD) << " ";
	    oss << ce.raw_bytes(DDIR_DOWNLOAD) << " ";
	    oss << ce.bytes(DDIR_UPLOAD) << " ";
	    oss << ce.bytes(DDIR_DOWNLOAD);
	    oss << "\r\n";
	}

	if (FileTransfer::subscribe_event_q.size() > 0)
	{
	    std::stringbuf *pbuf=oss.rdbuf();
	    size_t slen = strlen(pbuf->str().c_str());
	    send(cmdsock,pbuf->str().c_str(),slen,0);
	}
	FileTransfer::subscribe_event_q.clear();
    }

}



void CmdGwDataCameInCallback(struct bufferevent *bev, void *ctx)
{
	// Turn TCP stream into lines deliniated by \r\n

	evutil_socket_t cmdsock = bufferevent_getfd(bev);
	if (cmd_gw_debug)
		fprintf(stderr,"CmdGwDataCameIn: ENTER %d\n", cmdsock );

	struct evbuffer *inputevbuf = bufferevent_get_input(bev);

	int inlen = evbuffer_get_length(inputevbuf);

    int ret = evbuffer_add_buffer(cmd_evbuffer,inputevbuf);
	if (ret == -1) {
		CmdGwCloseConnection(cmdsock);
		return;
	}

	int totlen = evbuffer_get_length(cmd_evbuffer);

	if (cmd_gw_debug)
		fprintf(stderr,"cmdgw: TCPDataCameIn: State %d, got %d new bytes, have %d want %d\n", (int)cmd_tunnel_state, inlen, totlen, cmd_tunnel_expect );

	CmdGwProcessData(cmdsock);
}


void CmdGwProcessData(evutil_socket_t cmdsock)
{
	// Process CMD data in the cmd_evbuffer

	if (cmd_tunnel_state == CMDGW_TUNNEL_SCAN4CRLF)
	{
		bool ok=false;
		do
		{
			ok = CmdGwReadLine(cmdsock);
			if (ok && cmd_tunnel_state == CMDGW_TUNNEL_READTUNNEL)
				break;
		} while (ok);
	}
	// Not else!
	if (cmd_tunnel_state == CMDGW_TUNNEL_READTUNNEL)
	{
		// Got "TUNNELSEND addr size\r\n" command, now read
		// size bytes, i.e., cmd_tunnel_expect bytes.

		if (cmd_gw_debug)
			fprintf(stderr,"cmdgw: procTCPdata: tunnel state, got %d, want %d\n", evbuffer_get_length(cmd_evbuffer), cmd_tunnel_expect );

		if (evbuffer_get_length(cmd_evbuffer) >= cmd_tunnel_expect)
		{
			// We have all the tunneled data
			CmdGwTunnelSendUDP(cmd_evbuffer);

			// Process any remaining commands that came after the tunneled data
			CmdGwProcessData(cmdsock);
		}
	}
}


bool CmdGwReadLine(evutil_socket_t cmdsock)
{
	// Parse cmd_evbuffer for lines, and call NewRequest when found

	size_t rd=0;
    char *cmd = evbuffer_readln(cmd_evbuffer,&rd, EVBUFFER_EOL_CRLF_STRICT);
    if (cmd != NULL)
    {
    	CmdGwNewRequestCallback(cmdsock,cmd);
    	free(cmd);
    	return true;
    }
    else
    	return false;
}

int CmdGwHandleCommand(evutil_socket_t cmdsock, char *copyline);


void CmdGwNewRequestCallback(evutil_socket_t cmdsock, char *line)
{
	// New command received from user

    // CMD request line
	char *copyline = (char *)malloc(strlen(line)+1);
	strcpy(copyline,line);

	int ret = CmdGwHandleCommand(cmdsock,copyline);
	if (ret < 0) {
		dprintf("cmd: Error processing command %s\n", line );
		std::string msg = "";
		if (ret == ERROR_UNKNOWN_CMD)
			msg = "unknown command";
		else if (ret == ERROR_MISS_ARG)
			msg = "missing parameter";
		else if (ret == ERROR_BAD_ARG)
			msg = "bad parameter";
		// BAD_SWARM already sent, and not fatal

		if (msg != "")
		{
			CmdGwSendERRORBySocket(cmdsock,msg);
			CmdGwCloseConnection(cmdsock);
		}
	}

    free(copyline);
}





int CmdGwHandleCommand(evutil_socket_t cmdsock, char *copyline)
{
	char *method=NULL,*paramstr = NULL;
	char * token = strchr(copyline,' '); // split into CMD PARAM
	if (token != NULL) {
		*token = '\0';
		paramstr = token+1;
	}
	else
		paramstr = (char *)"";

	method = copyline;

	if (cmd_gw_debug)
		fprintf(stderr,"cmd: GOT %s %s\n", method, paramstr);

    char *savetok = NULL;
    if (!strcmp(method,"START"))
    {
    	// New START request
        //fprintf(stderr,"cmd: START: new request %i\n",cmd_gw_reqs_count+1);

        // Format: START url [destdir [metadir]]\r\n
        // Arno, 2012-04-13: See if URL followed by destpath for seeding
	// Arno, 2013-03-12: See if destpath followed by metadir
	std::string pstr = paramstr;
        std::string url="",storagepath="", metadir="";
        int sidx = pstr.find(" ");
        if (sidx == std::string::npos)
        {
            // No storage path or metadir
	    url = pstr;
        }
        else
        {
	    url = pstr.substr(0,sidx);
	    std::string qstr = pstr.substr(sidx+1);
	    sidx = qstr.find(" ");
	    if (sidx == std::string::npos)
	    {
		// Only storage path
		storagepath = qstr;
	    }
	    else
	    {
		// Storage path and metadir
		storagepath = qstr.substr(0,sidx);
		metadir = qstr.substr(sidx+1);
	    }
        }

        // If no metadir in command, but one is set on swift command line use latter.
        if (cmd_gw_metadir.compare("") && !metadir.compare(""))
            metadir = cmd_gw_metadir;
        if (metadir.length() > 0)
        {
            if (metadir.substr(metadir.length()-std::string(FILE_SEP).length()).compare(FILE_SEP))
                metadir += FILE_SEP;
        }

        // Parse URL
        parseduri_t puri;
        if (!swift::ParseURI(url,puri))
        	return ERROR_BAD_ARG;

        std::string trackerstr = puri["server"];
        std::string hashstr = puri["hash"];
        std::string mfstr = puri["filename"];
        std::string chunksizestr = puri["chunksizestr"];
        std::string durationstr = puri["durationstr"];

        if (hashstr.length()!=40) {
        	dprintf("cmd: START: roothash too short %i\n", hashstr.length() );
            return ERROR_BAD_ARG;
        }
        uint32_t chunksize=SWIFT_DEFAULT_CHUNK_SIZE;
        if (chunksizestr.length() > 0)
        	std::istringstream(chunksizestr) >> chunksize;
        int duration=0;
        if (durationstr.length() > 0)
        	std::istringstream(durationstr) >> duration;

        dprintf("cmd: START: %s with tracker %s chunksize %i duration %i storage %s metadir %s\n",hashstr.c_str(),trackerstr.c_str(),chunksize,duration,storagepath.c_str(),metadir.c_str() );

        // ARNOTODO: return duration in HTTPGW

        Address trackaddr;
	trackaddr = Address(trackerstr.c_str());
	if (trackaddr==Address())
	{
		dprintf("cmd: START: tracker address must be hostname:port, ip:port or just port\n");
	return ERROR_BAD_ARG;
	}
	// SetTracker(trackaddr); == set default tracker

        // initiate transmission
        Sha1Hash root_hash = Sha1Hash(true,hashstr.c_str());

        // Arno, 2012-06-12: Check for duplicate requests
        cmd_gw_t* req = CmdGwFindRequestByRootHash(root_hash);
        if (req != NULL)
        {
        	dprintf("cmd: START: request for given root hash already exists\n");
        	return ERROR_BAD_ARG;
        }

        // Send INFO DLSTATUS_HASHCHECKING
	CmdGwSendINFOHashChecking(cmdsock,root_hash);

	// ARNOSMPTODO: disable/interleave hashchecking at startup
        int transfer = swift::Find(root_hash);
        if (transfer==-1) {
        	std::string filename;
        	if (storagepath != "")
        		filename = storagepath;
        	else
        		filename = hashstr;
            transfer = swift::Open(filename,root_hash,metadir,trackaddr,false,true,chunksize);
            if (transfer == -1)
            {
            	CmdGwSendERRORBySocket(cmdsock,"bad swarm",root_hash);
            	return ERROR_BAD_SWARM;
            }
        }

        // All is well, register req
        req = cmd_requests + cmd_gw_reqs_open++;
        req->id = ++cmd_gw_reqs_count;
        req->cmdsock = cmdsock;
        req->transfer = transfer;
        req->startt = usec_time();
        req->mfspecname = mfstr;

        dprintf("%s @%i start transfer %i\n",tintstr(),req->id,req->transfer);

        // RATELIMIT
        //FileTransfer::file(transfer)->SetMaxSpeed(DDIR_DOWNLOAD,512*1024);

        if (cmd_gw_debug)
        	fprintf(stderr,"cmd: Already on disk is %lli/%lli\n", swift::Complete(transfer), swift::Size(transfer));

        // MULTIFILE
        int64_t minsize=CMDGW_MAX_PREBUF_BYTES;
        FileTransfer *ft = FileTransfer::file(transfer);
        if (ft == NULL)
        	return ERROR_BAD_ARG;
		storage_files_t sfs = ft->GetStorage()->GetStorageFiles();
		if (sfs.size() > 0)
			minsize = sfs[0]->GetSize();

        // Wait for prebuffering and then send PLAY to user
        if (swift::SeqComplete(transfer) >= minsize)
        {
            CmdGwSwiftFirstProgressCallback(transfer,bin_t(0,0));
            CmdGwSendINFO(req, DLSTATUS_DOWNLOADING);
        }
        else
        {
            swift::AddProgressCallback(transfer,&CmdGwSwiftFirstProgressCallback,CMDGW_FIRST_PROGRESS_BYTE_INTERVAL_AS_LAYER);
        }

        ft->GetStorage()->AddOneTimeAllocationCallback(CmdGwSwiftAllocatingDiskspaceCallback);
    }
    else if (!strcmp(method,"REMOVE"))
    {
    	// REMOVE roothash removestate removecontent\r\n
    	bool removestate = false, removecontent = false;

        token = strtok_r(paramstr," ",&savetok); //
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *hashstr = token;
        token = strtok_r(NULL," ",&savetok);      // removestate
        if (token == NULL)
        	return ERROR_MISS_ARG;
        removestate = !strcmp(token,"1");
        token = strtok_r(NULL,"",&savetok);       // removecontent
        if (token == NULL)
        	return ERROR_MISS_ARG;
        removecontent = !strcmp(token,"1");

    	Sha1Hash root_hash = Sha1Hash(true,hashstr);
    	CmdGwGotREMOVE(root_hash,removestate,removecontent);
    }
    else if (!strcmp(method,"MAXSPEED"))
    {
    	// MAXSPEED roothash direction speed-float-kb/s\r\n
    	data_direction_t ddir;
    	double speed;

        token = strtok_r(paramstr," ",&savetok); //
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *hashstr = token;
        token = strtok_r(NULL," ",&savetok);      // direction
        if (token == NULL)
        	return ERROR_MISS_ARG;
        ddir = !strcmp(token,"DOWNLOAD") ? DDIR_DOWNLOAD : DDIR_UPLOAD;
        token = strtok_r(NULL,"",&savetok);       // speed
        if (token == NULL)
        	return ERROR_MISS_ARG;
        int n = sscanf(token,"%lf",&speed);
        if (n == 0) {
        	dprintf("cmd: MAXSPEED: speed is not a float\n");
			return ERROR_MISS_ARG;
        }
    	Sha1Hash root_hash = Sha1Hash(true,hashstr);
    	CmdGwGotMAXSPEED(root_hash,ddir,speed*1024.0);
    }
    else if (!strcmp(method,"CHECKPOINT"))
    {
    	// CHECKPOINT roothash\r\n
    	Sha1Hash root_hash = Sha1Hash(true,paramstr);
    	CmdGwGotCHECKPOINT(root_hash);
    }
    else if (!strcmp(method,"SETMOREINFO"))
    {
    	// GETMOREINFO roothash toggle\r\n
        token = strtok_r(paramstr," ",&savetok); // hash
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *hashstr = token;
        token = strtok_r(NULL," ",&savetok);      // bool
        if (token == NULL)
        	return ERROR_MISS_ARG;
        bool enable = (bool)!strcmp(token,"1");
    	Sha1Hash root_hash = Sha1Hash(true,hashstr);
    	CmdGwGotSETMOREINFO(root_hash,enable);
    }
    else if (!strcmp(method,"RECIPROCITY"))
    {
        // RECIPROCITY message\r\n
        token = strtok_r(paramstr," ",&savetok); // hash
        if (token == NULL)
            return ERROR_MISS_ARG;
        CmdGwGotRECIPROCITY(token);
    }
    else if (!strcmp(method,"SHUTDOWN"))
    {
    	CmdGwCloseConnection(cmdsock);
    	// Tell libevent to stop processing events
    	event_base_loopexit(Channel::evbase, NULL);
    }
    else if (!strcmp(method,"TUNNELSEND"))
    {
        token = strtok_r(paramstr,"/",&savetok); // dest addr
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *addrstr = token;
        token = strtok_r(NULL," ",&savetok);      // channel
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *chanstr = token;
        token = strtok_r(NULL," ",&savetok);      // size
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *sizestr = token;

    	cmd_tunnel_dest_addr = Address(addrstr);
    	int n = sscanf(chanstr,"%08x",&cmd_tunnel_dest_chanid);
    	if (n != 1)
    		return ERROR_BAD_ARG;
    	n = sscanf(sizestr,"%u",&cmd_tunnel_expect);
    	if (n != 1)
    		return ERROR_BAD_ARG;

        cmd_tunnel_state = CMDGW_TUNNEL_READTUNNEL;

        if (cmd_gw_debug)
        	fprintf(stderr,"cmdgw: Want tunnel %d bytes to %s\n", cmd_tunnel_expect, cmd_tunnel_dest_addr.str() );
    }
    else if (!strcmp(method,"PEERADDR"))
    {
    	// PEERADDR roothash addrstr\r\n
        token = strtok_r(paramstr," ",&savetok); // hash
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *hashstr = token;
        token = strtok_r(NULL," ",&savetok);      // bool
        if (token == NULL)
        	return ERROR_MISS_ARG;
        char *addrstr = token;
        Address peer(addrstr);
    	Sha1Hash root_hash = Sha1Hash(true,hashstr);
    	CmdGwGotPEERADDR(root_hash,peer);
    }
    else if (!strcmp(method,"SUBSCRIBE"))
    {
	// Format: SUBSCRIBE swarmid EVENT 1/0
	//         SUBSCRIBE 089aa... CHANNEL_CLOSE 1
	std::string pstr = paramstr;
        std::string swarmidstr="",eventstr="", enablestr="";
        int sidx = pstr.find(" ");
        if (sidx == std::string::npos)
        {
            return ERROR_MISS_ARG;
        }
        else
        {
	    swarmidstr = pstr.substr(0,sidx);
	    std::string qstr = pstr.substr(sidx+1);
	    sidx = qstr.find(" ");
	    if (sidx == std::string::npos)
	    {
		// On/off missing
		return ERROR_MISS_ARG;
	    }
	    else
	    {
		eventstr = qstr.substr(0,sidx);
		enablestr = qstr.substr(sidx+1);
	    }

        }
        if (swarmidstr.compare("ALL") || eventstr.compare("CHANNEL_CLOSE"))
            return ERROR_BAD_ARG;

        FileTransfer::subscribe_channel_close = !enablestr.compare("1");
        if (!FileTransfer::subscribe_channel_close)
            FileTransfer::subscribe_event_q.clear();
    }
    else
    {
    	return ERROR_UNKNOWN_CMD;
    }

    return ERROR_NO_ERROR;
}



void CmdGwEventCameInCallback(struct bufferevent *bev, short events, void *ctx)
{
	if (events & BEV_EVENT_ERROR)
		print_error("cmdgw: Error from bufferevent");
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR))
    {
    	// Called when error on cmd connection
    	evutil_socket_t cmdsock = bufferevent_getfd(bev);
    	CmdGwCloseConnection(cmdsock);
		bufferevent_free(bev);
    }
}


void CmdGwNewConnectionCallback(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen,
    void *ctx)
{
    // New TCP connection on cmd listen socket

    fprintf(stderr,"cmd: Got new cmd connection %i\n",fd);

    struct event_base *base = evconnlistener_get_base(listener);
    struct bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);

    bufferevent_setcb(bev, CmdGwDataCameInCallback, NULL, CmdGwEventCameInCallback, NULL);
    bufferevent_enable(bev, EV_READ|EV_WRITE);

    // ARNOTODO: free bufferevent when conn closes.

    // One buffer for all cmd connections, reset
    if (cmd_evbuffer != NULL)
	evbuffer_free(cmd_evbuffer);
    cmd_evbuffer = evbuffer_new();

    // SOCKTUNNEL: assume 1 command connection
    cmd_tunnel_sock = fd;

    cmd_gw_conns_open++;
}


void CmdGwListenErrorCallback(struct evconnlistener *listener, void *ctx)
{
	// libevent got error on cmd listener

    fprintf(stderr,"CmdGwListenErrorCallback: Something wrong with CMDGW\n" );
    struct event_base *base = evconnlistener_get_base(listener);
    int err = EVUTIL_SOCKET_ERROR();
    char errmsg[1024];
    sprintf(errmsg, "cmdgw: Got a fatal error %d (%s) on the listener.\n", err, evutil_socket_error_to_string(err));

    print_error(errmsg);
    dprintf("%s @0 closed cmd gateway\n",tintstr());

	evconnlistener_free(cmd_evlistener);
}


bool InstallCmdGateway (struct event_base *evbase,Address cmdaddr,Address httpaddr, std::string metadir)
{
	// Allocate libevent listener for cmd connections
	// From http://www.wangafu.net/~nickm/libevent-book/Ref8_listener.html

    fprintf(stderr,"cmdgw: Creating new TCP listener on addr %s\n", cmdaddr.str() );
  
    const struct sockaddr_in sin = (sockaddr_in)cmdaddr;

    cmd_evlistener = evconnlistener_new_bind(evbase, CmdGwNewConnectionCallback, NULL,
        LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE, -1,
        (const struct sockaddr *)&sin, sizeof(sin));
    if (!cmd_evlistener) {
            print_error("Couldn't create listener");
            return false;
    }
    evconnlistener_set_error_cb(cmd_evlistener, CmdGwListenErrorCallback);

    cmd_gw_httpaddr = httpaddr;
    cmd_gw_metadir = metadir;

    cmd_evbuffer = evbuffer_new();

    return true;
}



// SOCKTUNNEL
void swift::CmdGwTunnelUDPDataCameIn(Address srcaddr, uint32_t srcchan, struct evbuffer* evb)
{
	// Message received on UDP socket, forward over TCP conn.

	if (cmd_gw_debug)
		fprintf(stderr,"cmdgw: TunnelUDPData:DataCameIn %d bytes from %s/%08x\n", evbuffer_get_length(evb), srcaddr.str(), srcchan );

	/*
	 *  Format:
	 *  TUNNELRECV ip:port/hexchanid nbytes\r\n
	 *  <bytes>
	 */

    std::ostringstream oss;
    oss << "TUNNELRECV " << srcaddr.str();
    oss << "/" << std::hex << srcchan;
    oss << " " << std::dec << evbuffer_get_length(evb) << "\r\n";

	std::stringbuf *pbuf=oss.rdbuf();
	size_t slen = strlen(pbuf->str().c_str());
	send(cmd_tunnel_sock,pbuf->str().c_str(),slen,0);

	slen = evbuffer_get_length(evb);
	uint8_t *data = evbuffer_pullup(evb,slen);
	send(cmd_tunnel_sock,(const char *)data,slen,0);

	evbuffer_drain(evb,slen);
}


void swift::CmdGwTunnelSendUDP(struct evbuffer *evb)
{
	// Received data from TCP connection, send over UDP to specified dest
	cmd_tunnel_state = CMDGW_TUNNEL_SCAN4CRLF;

	if (cmd_gw_debug)
		fprintf(stderr,"cmdgw: sendudp:");

	struct evbuffer *sendevbuf = evbuffer_new();

	// Add channel id. Currently always CMDGW_TUNNEL_DEFAULT_CHANNEL_ID=0xffffffff
	// but we may add a TUNNELSUBSCRIBE command later to allow the allocation
	// of different channels for different TCP clients.
	int ret = evbuffer_add_32be(sendevbuf, cmd_tunnel_dest_chanid);
	if (ret < 0)
	{
		evbuffer_drain(evb,cmd_tunnel_expect);
		evbuffer_free(sendevbuf);
		fprintf(stderr,"cmdgw: sendudp :can't copy prefix to sendbuf!");
		return;
	}
	ret = evbuffer_remove_buffer(evb, sendevbuf, cmd_tunnel_expect);
	if (ret < 0)
	{
		evbuffer_drain(evb,cmd_tunnel_expect);
		evbuffer_free(sendevbuf);
		fprintf(stderr,"cmdgw: sendudp :can't copy to sendbuf!");
		return;
	}
	if (Channel::sock_count != 1)
	{
		fprintf(stderr,"cmdgw: sendudp: no single UDP socket!");
		evbuffer_free(sendevbuf);
		return;
	}
	evutil_socket_t sock = Channel::sock_open[Channel::sock_count-1].sock;

	Channel::SendTo(sock,cmd_tunnel_dest_addr,sendevbuf);

	evbuffer_free(sendevbuf);
}
