/*
 *  sendrecv.cpp
 *  most of the swift's state machine
 *
 *  Created by Victor Grishchenko on 3/6/09.
 *  Copyright 2009-2016 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */
// Arno, 2013-06-11: Must come first to ensure SIZE_MAX etc are defined
#include "compat.h"
#include "bin_utils.h"
#include "swift.h"
#include <algorithm>  // kill it
#include <cassert>
#include <cfloat>
#include <sstream>

using namespace swift;
using namespace std;

struct event_base *Channel::evbase;
struct event Channel::evrecv;

#define DEBUGTRAFFIC     0

/** Arno: Victor's design allows a sender to choose some data to push to
 * a receiver, if that receiver is not HINTing at data. Should be disabled
 * when the receiver has a download rate limit.
 */
#define ENABLE_SENDERSIZE_PUSH 0


#define ENABLE_CANCEL 		0


/** Arno, 2011-11-24: When rate limit is on and the download is in progress
 * we send HINTs for 2 chunks at the moment. This constant can be used to
 * get greater granularity. Set to 0 for original behavior.
 * Ric: set to 2, it should be smaller than 4 or we need to change the hint
 * strategy first
 * Ric: 2013-05: We need to always send out at least one req. for each channel.
 * Otherwise, if the link between two peers is heavily congested or has a high
 * pkt loss rate, ledbat will close the connection. (short explanation)
 */
#define HINT_GRANULARITY    1 // chunks

/** Arno, 2012-03-16: Swift can now tunnel data from CMDGW over UDP to
 * CMDGW at another swift instance. This is the default channel ID on UDP
 * for that traffic (cf. overlay swarm).
 */
#define CMDGW_TUNNEL_DEFAULT_CHANNEL_ID        0xffffffff






void    Channel::AddPeakHashes (struct evbuffer *evb) {
    for(int i=0; i<hashtree()->peak_count(); i++) {
        bin_t peak = hashtree()->peak(i);
        evbuffer_add_8(evb, SWIFT_INTEGRITY);
        evbuffer_add_chunkaddr(evb,peak,hs_out_->chunk_addr_);
        evbuffer_add_hash(evb, hashtree()->peak_hash(i));
        dprintf("%s #%u +phash %s\n",tintstr(),id_,peak.str().c_str());
    }
}


void    Channel::AddUncleHashes (struct evbuffer *evb, bin_t pos) {

    dprintf("%s #%u +uncle hash for %s\n",tintstr(),id_,pos.str().c_str());
    bin_t peak = hashtree()->peak_for(pos);
    // Ric: TODO check (remove data_out_cap??)
    //      For the moment lets keep the old behaviour
    binvector bv;
    while (pos!=peak && ((NOW&3)==3 || !pos.parent().contains(data_out_cap_)) &&
            ack_in_.is_empty(pos.parent()) ) {
    //while (pos!=peak && ack_in_.is_empty(pos.parent()) ) {
        bin_t uncle = pos.sibling();
        bv.push_back(uncle);
        pos = pos.parent();
    }
    // PPSP -04: Send in descending layer order
    binvector::reverse_iterator iter;
    for (iter=bv.rbegin(); iter != bv.rend(); iter++) {
        bin_t uncle = *iter;
        evbuffer_add_8(evb, SWIFT_INTEGRITY);
        evbuffer_add_chunkaddr(evb,uncle,hs_out_->chunk_addr_);
        evbuffer_add_hash(evb, hashtree()->hash(uncle) );
        dprintf("%s #%u +hash %s\n",tintstr(),id_,uncle.str().c_str());
    }

}


bin_t    Channel::ImposeHint () {
    uint64_t twist = hs_in_->peer_channel_id_;  // got no hints, send something randomly

    twist &= hashtree()->peak(0).toUInt(); // FIXME may make it semi-seq here

    bin_t my_pick = binmap_t::find_complement(ack_in_, *(transfer()->ack_out()), twist);
    my_pick.to_twisted(twist);
    while (my_pick.base_length()>max(1,(int)cwnd_))
        my_pick = my_pick.left();

    return my_pick.twisted(twist);
}


bin_t        Channel::DequeueHint (bool *retransmitptr) {
    bin_t send = bin_t::NONE;

    // Arno, 2012-01-23: Extra protection against channel loss, don't send DATA
    if (last_recv_time_ < NOW-(3*TINT_SEC))
    {
    	dprintf("%s #%u dequeued bad time %lld\n",tintstr(),id_, last_recv_time_ );
    	return bin_t::NONE;
    }

    // Arno, 2012-07-27: Reenable Victor's retransmit, check for ACKs
    *retransmitptr = false;
    while (!data_out_tmo_.empty()) {
        tintbin tb = data_out_tmo_.front();
        data_out_tmo_.pop_front();
        if (ack_in_.is_filled(tb.bin)) {
	    // chunk was acknowledged in meantime
	    continue;
	}
	else {
	    send = tb.bin;
	    *retransmitptr = true;
	    break;
	}
    }

    if (ENABLE_SENDERSIZE_PUSH && send.is_none() && hint_in_.empty() && last_recv_time_>NOW-rtt_avg_-TINT_SEC) {
        bin_t my_pick = ImposeHint(); // FIXME move to the loop
        if (!my_pick.is_none()) {
        	hint_in_size_ += my_pick.base_offset();
            hint_in_.push_back(my_pick);
            dprintf("%s #%u *hint %s\n",tintstr(),id_,my_pick.str().c_str());
        }
    }
    
    while (!hint_in_.empty() && send.is_none()) {
        bin_t hint = hint_in_.front().bin;
        tint time = hint_in_.front().time;
        hint_in_size_ -= hint_in_.front().bin.base_length();
        hint_in_.pop_front();
        while (!hint.is_base()) { // FIXME optimize; possible attack
            hint_in_.push_front(tintbin(time,hint.right()));
            hint_in_size_ += hint_in_.front().bin.base_length();
            hint = hint.left();
        }
        //if (time < NOW-TINT_SEC*3/2 )
        //    continue;  bad idea
        if (!ack_in_.is_filled(hint))
            send = hint;
    }

    dprintf("%s #%u dequeued %s [%llu]\n",tintstr(),id_,send.str().c_str(),hint_in_size_);
    return send;
}


void    Channel::AddHandshake (struct evbuffer *evb)
{
    // If peer not responding, try legacy swift protocol
#if ENABLE_FALLBACK_TO_LEGACY_PROTO == 1
    if (sent_since_recv_ >= 3 && last_recv_time_ == 0)
	hs_out_->ResetToLegacy();
#endif

    int encoded = -1;
    if (hs_out_->version_ == VER_SWIFT_LEGACY)
    {
	//dprintf("%s #%u +hs swift legacy\n",tintstr(),id_ );
	if (hs_in_ == NULL) { // initiating
	    evbuffer_add_8(evb, SWIFT_INTEGRITY);
	    evbuffer_add_32be(evb, bin_toUInt32(bin_t::ALL));
	    evbuffer_add_hash(evb, transfer()->swarm_id());
	    dprintf("%s #%u +hash ALL %s\n",
		    tintstr(),id_,transfer()->swarm_id().hex().c_str());
	}
	evbuffer_add_8(evb, SWIFT_HANDSHAKE);

	if (send_control_==CLOSE_CONTROL) {
	    encoded = 0;
	}
	else
	    encoded = EncodeID(id_);
	evbuffer_add_32be(evb, encoded);

	dprintf("%s #%u +hs %x swift\n",tintstr(),id_,encoded);
    }
    else // IETF PPSP compliant
    {
	//dprintf("%s #%u +hs ppsp\n",tintstr(),id_ );
	evbuffer_add_8(evb, SWIFT_HANDSHAKE);
	if (send_control_==CLOSE_CONTROL) {
	    encoded = 0;
	}
	else
	    encoded = EncodeID(id_);
	evbuffer_add_32be(evb, encoded);

	// Send protocol options
	std::ostringstream cross;
	if (send_control_ !=CLOSE_CONTROL) {
	    evbuffer_add_8(evb, POPT_VERSION);
	    evbuffer_add_8(evb, hs_out_->version_);
	    cross << "v" << hs_out_->version_ << " ";
	    evbuffer_add_8(evb, POPT_MIN_VERSION);
	    evbuffer_add_8(evb, hs_out_->min_version_);
	    cross << "nv" << hs_out_->version_ << " ";

	    if (hs_in_ == NULL) { // initiating, send swarm ID
		evbuffer_add_8(evb, POPT_SWARMID);
		evbuffer_add_16be(evb, Sha1Hash::SIZE); // PPSPTODO LIVE
		evbuffer_add_hash(evb, transfer()->swarm_id() );
		cross << "sid " << transfer()->swarm_id().hex() << " ";
	    }
	    evbuffer_add_8(evb, POPT_CONT_INT_PROT);
	    evbuffer_add_8(evb, hs_out_->cont_int_prot_);
	    cross << "cipm " << hs_out_->cont_int_prot_ << " ";
	    if (hs_out_->cont_int_prot_ == POPT_CONT_INT_PROT_MERKLE)
	    {
		evbuffer_add_8(evb, POPT_MERKLE_HASH_FUNC);
		evbuffer_add_8(evb, hs_out_->merkle_func_);
		cross << "mhf " << hs_out_->merkle_func_ << " ";
	    }
	    evbuffer_add_8(evb, POPT_CHUNK_ADDR);
	    evbuffer_add_8(evb, hs_out_->chunk_addr_);
	    cross << "cam " << hs_out_->chunk_addr_ << " ";
	    if (transfer()->ttype() == LIVE_TRANSFER)
	    {
		evbuffer_add_8(evb, POPT_LIVE_DISC_WND);
		// For POPT_CHUNK_ADDR_CHUNK32, saves all chunks
		// PPSPTODO forget
		if (hs_out_->chunk_addr_ == POPT_CHUNK_ADDR_BIN32 || hs_out_->chunk_addr_ == POPT_CHUNK_ADDR_CHUNK32)
		    evbuffer_add_32be(evb, (uint32_t)hs_out_->live_disc_wnd_);
		else
		    evbuffer_add_64be(evb, hs_out_->live_disc_wnd_);
		cross << "ldw " << std::hex << hs_out_->live_disc_wnd_ << std::dec << " ";
	    }
	}
	dprintf("%s #%u +hs %x ppsp %s\n",tintstr(),id_,encoded, cross.str().c_str() );

	evbuffer_add_8(evb, POPT_END);
    }

    have_out_.clear();
}


void    Channel::Send () {

    dprintf("%s #%u Send called \n",tintstr(),id_);

    struct evbuffer *evb = evbuffer_new();
    uint32_t pcid = 0;
    if (hs_in_ != NULL)
	pcid =  hs_in_->peer_channel_id_;

    evbuffer_add_32be(evb,pcid);
    bin_t data = bin_t::NONE;
    int evbnonadplen = 0;
    if (send_control_==CLOSE_CONTROL) // Arno: send explicit close
        AddHandshake(evb);
    else
    {
	if (is_established()) {
            // FIXME: seeder check
            AddHave(evb);
            AddAck(evb);
            //LIVE
            if (hashtree() == NULL || !hashtree()->is_complete()) {
                AddHint(evb);
                /* Gertjan fix: 7aeea65f3efbb9013f601b22a57ee4a423f1a94d
                "Only call Reschedule for 'reverse PEX' if the channel is in keep-alive mode"
                 */
                AddPexReq(evb);
                if (ENABLE_CANCEL)
                    AddCancel(evb);
            }
            AddPex(evb);
            TimeoutDataOut();
            data = AddData(evb);
        } else  {
            AddHandshake(evb);
            AddHave(evb); // Arno, 2011-10-28: from AddHandShake. Why double?
            AddHave(evb);
            AddAck(evb);
        }
    }
    lastsendwaskeepalive_ = (evbuffer_get_length(evb) == 4);

    if (evbuffer_get_length(evb)==4) {// only the channel id; bare keep-alive
        data = bin_t::ALL;
    }
    dprintf("%s #%u sent %ib %s:%x\n",
            tintstr(),id_,(int)evbuffer_get_length(evb),peer().str().c_str(),
            pcid);

    int r = SendTo(socket_,peer(),evb);
    if (r==-1)
        print_error("swift can't send datagram");
    else
        raw_bytes_up_ += r;
    last_send_time_ = NOW;
    sent_since_recv_++;
    dgrams_sent_++;
    evbuffer_free(evb);
    Reschedule();
}

void    Channel::AddHint (struct evbuffer *evb) {

    // RATELIMIT
    // Policy is to not send hints when we are above speed limit
    if (transfer()->GetCurrentSpeed(DDIR_DOWNLOAD) > transfer()->GetMaxSpeed(DDIR_DOWNLOAD)) {
        if (DEBUGTRAFFIC)
            fprintf(stderr,"hint: forbidden#");
        return;
    }

    // 1. Calc max of what we are allowed to request, uncongested bandwidth wise
    tint plan_for = max(TINT_SEC*HINT_TIME,rtt_avg_*4);
    tint timed_out = NOW - plan_for*2;

    std::deque<bin_t> tbc;
    while ( !hint_out_.empty() && hint_out_.front().time < timed_out ) {
    	bin_t hint = hint_out_.front().bin;
        hint_out_size_ -= hint.base_length();
        hint_out_.pop_front();
        // Ric: keep track of what we want to remove
        tbc.push_back(hint);
	dprintf("%s #%u remove hint %s\n",tintstr(),id_,hint.str().c_str());

    }

    int first_plan_pck = max ( (tint)1, plan_for / dip_avg_ );

    // Riccardo, 2012-04-04: Actually allowed is max minus what we already asked for
    int queue_allowed_hints = max(0,first_plan_pck-(int)hint_out_size_);


    // RATELIMIT
    // 2. Calc max of what is allowed by the rate limiter
    int rate_allowed_hints = LONG_MAX;
    uint64_t rough_global_hint_out_size = 0; // rough estimate, as hint_out_ clean up is not done for all channels
    bool count_hints = false;
    if (transfer()->GetMaxSpeed(DDIR_DOWNLOAD) < DBL_MAX)
    {
        channels_t::iterator iter;
        for (iter=transfer()->GetChannels()->begin(); iter!=transfer()->GetChannels()->end(); iter++)
        {
            Channel *c = *iter;
            if (c != NULL)
            rough_global_hint_out_size += c->hint_out_size_;
        }

        // Policy: this channel is allowed to hint at the limit - global_hinted_at
        // Handle MaxSpeed = unlimited
        double rate_hints_limit_float = HINT_TIME*transfer()->GetMaxSpeed(DDIR_DOWNLOAD)/((double)transfer()->chunk_size());

		// Ric: slow down if we just started the connection
		double slowStart = (double)LONG_MAX;
		tint running = now_t::now-start;
		// It takes ~3 sec to get a stable DL speed estimation
		if (running<TINT_SEC*3 && hint_out_size_>1) {
			count_hints = true;
			slowStart = rate_hints_limit_float*running/TINT_SEC;
			if (slowStart>transfer()->GetSlowStartHints())
				slowStart = slowStart-transfer()->GetSlowStartHints();
			else
				slowStart = 0;
			if (DEBUGTRAFFIC)
				fprintf(stderr, "slowStart: %ld [%u]\n", slowStart, transfer()->GetSlowStartHints());
		}
		
        int rate_hints_limit = (int)min(slowStart,rate_hints_limit_float);

        // Actually allowed is max minus what we already asked for, globally (=all channels)
        rate_allowed_hints = max(0,rate_hints_limit-(int)rough_global_hint_out_size);
    }
    if (DEBUGTRAFFIC)
    	fprintf(stderr,"hint c%u: %lf want %d qallow %d rallow %d chanout %llu globout %llu\n", id(), transfer()->GetCurrentSpeed(DDIR_DOWNLOAD), first_plan_pck, queue_allowed_hints, rate_allowed_hints, hint_out_size_, rough_global_hint_out_size );

    // 3. Take the smallest allowance from rate and queue limit
    uint64_t plan_pck = (uint64_t)min(rate_allowed_hints,queue_allowed_hints);

    // 4. Ask allowance in blocks of chunks to get pipelining going from serving peer.
    // Arno, 2012-10-30: not HINT_GRANULARITY for LIVE
    if (hint_out_size_ == 0 || plan_pck >= HINT_GRANULARITY || transfer()->ttype() == LIVE_TRANSFER)
    {
        bin_t hint = transfer()->picker()->Pick(ack_in_,plan_pck,NOW+plan_for*2);
        if (!hint.is_none()) {
            if (DEBUGTRAFFIC)
            {
                fprintf(stderr,"hint c%d: ask %s\n", id(), hint.str().c_str() );
            }
            evbuffer_add_8(evb, SWIFT_REQUEST);
            evbuffer_add_chunkaddr(evb,hint,hs_out_->chunk_addr_);
            dprintf("%s #%u +hint %s [%lld]\n",tintstr(),id_,hint.str().c_str(),hint_out_size_);
            dprintf("%s #%u +hint base %s width %d\n",tintstr(),id_,hint.base_left().str().c_str(), (int)hint.base_length() );
            //fprintf(stderr,"send c%d: HINTLEN %i\n", id(), hint.base_length());
            //fprintf(stderr,"HL %i ", hint.base_length());

#if ENABLE_CANCEL == 1
            // Ric: final cancel the hints that have been removed
            while (!tbc.empty()) {
                bin_t c = tbc.front();
                if (!c.contains(hint) && !hint.contains(c))
	                cancel_out_.push_back(c);
                else if (c.contains(hint))
	                while (c.contains(hint) && c!=hint) {
		                if (c>hint)
			                c.to_left();
		                else
			                c.to_right();
		                cancel_out_.push_back(c.sibling());
	                }
                else if (c == hint)
	                break;
                tbc.pop_front();
            }
#endif
            hint_out_.push_back(hint);
            hint_out_size_ += hint.base_length();

            // Ric: keep track of the outstanding hints
            if (count_hints)
            	transfer()->SetSlowStartHints(hint.base_length());
            return;
        }
        else
            dprintf("%s #%u Xhint\n",tintstr(),id_);
    }
#if ENABLE_CANCEL == 1
    // add the temporary cancel bin to the actual cancel queue
	while (!tbc.empty()) {
		cancel_out_.push_back(tbc.front());
		tbc.pop_front();
    }
#endif
}

static int ChunkAddrSize(popt_chunk_addr_t ca)
{
    switch(ca)
    {
        case POPT_CHUNK_ADDR_BIN32:
            return 4;
        case POPT_CHUNK_ADDR_BYTE64:
            return 2*8;
        case POPT_CHUNK_ADDR_CHUNK32:
            return 2*4;
        case POPT_CHUNK_ADDR_BIN64:
            return 8;
        case POPT_CHUNK_ADDR_CHUNK64:
            return 2*8;
    }
    return 0;
}

void    Channel::AddCancel (struct evbuffer *evb) {

	// Arno, 2013-01-15: take into account chunk addressing scheme
	while (SWIFT_MAX_NONDATA_DGRAM_SIZE-evbuffer_get_length(evb) >= 1+ChunkAddrSize(hs_out_->chunk_addr_) && !cancel_out_.empty()) {
		bin_t cancel = cancel_out_.front();
		cancel_out_.pop_front();
		evbuffer_add_8(evb, SWIFT_CANCEL);
                evbuffer_add_chunkaddr(evb,cancel,hs_out_->chunk_addr_);
		dprintf("%s #%u +cancel %s\n",
			tintstr(),id_,cancel.str().c_str());
	}
}

bin_t        Channel::AddData (struct evbuffer *evb) {
    // RATELIMIT
    if (transfer()->GetCurrentSpeed(DDIR_UPLOAD) > transfer()->GetMaxSpeed(DDIR_UPLOAD)) {
        transfer()->OnSendNoData();
        return bin_t::NONE;
    }
    //LIVE
    if (transfer()->ttype() == FILE_TRANSFER && !hashtree()->size()) // know nothing
        return bin_t::NONE;

    // Arno: If we are serving content, keep activated
    swift::Touch(transfer()->td());

    bin_t tosend = bin_t::NONE;
    bool isretransmit = false;
    tint luft = send_interval_>>4; // may wake up a bit earlier
    if (data_out_.size()<cwnd_ &&
            last_data_out_time_+send_interval_<=NOW+luft) {
        tosend = DequeueHint(&isretransmit);
        if (tosend.is_none()) {
            dprintf("%s #%u sendctrl no idea what data to send\n",tintstr(),id_);
            if (send_control_!=KEEP_ALIVE_CONTROL && send_control_!=CLOSE_CONTROL)
                SwitchSendControl(KEEP_ALIVE_CONTROL);
        }
    } else
        dprintf("%s #%u sendctrl wait cwnd %f data_out %i next %s\n",
                tintstr(),id_,cwnd_,(int)data_out_.size(),tintstr(last_data_out_time_+send_interval_));

    if (tosend.is_none()) {// && (last_data_out_time_>NOW-TINT_SEC || data_out_.empty()))
        transfer()->OnSendNoData();
        return bin_t::NONE; // once in a while, empty data is sent just to check rtt FIXED
    }

    //LIVE
    if (transfer()->ttype() == FILE_TRANSFER) {
        if (ack_in_.is_empty() && hashtree()->size())
            AddPeakHashes(evb);
        //NETWVSHASH
        if (hashtree()->get_check_netwvshash())
            AddUncleHashes(evb,tosend);
    }

    if (!ack_in_.is_empty()) // TODO: cwnd_>1
        data_out_cap_ = tosend;

    // Arno, 2011-11-03: May happen when first data packet is sent to empty
    // leech, then peak + uncle hashes may be so big that they don't fit in eth
    // frame with DATA. Send 2 datagrams then, one with peaks so they have
    // a better chance of arriving. Optimistic violation of atomic datagram
    // principle.
    if (transfer()->chunk_size() == SWIFT_DEFAULT_CHUNK_SIZE && evbuffer_get_length(evb) > SWIFT_MAX_NONDATA_DGRAM_SIZE) {
        dprintf("%s #%u fsent %ib %s:%x\n",
                tintstr(),id_,(int)evbuffer_get_length(evb),peer().str().c_str(),
                hs_in_->peer_channel_id_);
        int ret = Channel::SendTo(socket_,peer(),evb); // kind of fragmentation
        if (ret > 0)
            raw_bytes_up_ += ret;
        evbuffer_add_32be(evb, hs_in_->peer_channel_id_);
    }
    evbuffer_add_8(evb, SWIFT_DATA);
    evbuffer_add_chunkaddr(evb,tosend,hs_out_->chunk_addr_);
    // PPSPTODO LEDBAT current system time 64-bit
    if (hs_in_ != NULL && hs_in_->version_ == VER_PPSPP_v1)
    {
	// NOTE: Time updates NOW, so customary behavior where NOW is not
	// updated during the handling of a message (just at start) is no longer
	// there. Not sure if this matters.
        evbuffer_add_64be(evb, Time() );
    }


    struct evbuffer_iovec vec;
    if (evbuffer_reserve_space(evb, transfer()->chunk_size(), &vec, 1) < 0) {
        print_error("error on evbuffer_reserve_space");
        return bin_t::NONE;
    }

    ssize_t r = transfer()->GetStorage()->Read((char *)vec.iov_base,
             transfer()->chunk_size(),tosend.base_offset()*transfer()->chunk_size());
    // TODO: corrupted data, retries, caching
    if (r<0) {
        print_error("error on reading");
        vec.iov_len = 0;
        evbuffer_commit_space(evb, &vec, 1);
        return bin_t::NONE;
    }
    // assert(dgram.space()>=r+4+1);
    vec.iov_len = r;
    if (evbuffer_commit_space(evb, &vec, 1) < 0) {
        print_error("error on evbuffer_commit_space");
        return bin_t::NONE;
    }

    last_data_out_time_ = NOW;
    data_out_.push_back(tosend);
    bytes_up_ += r;
    global_bytes_up += r;

    dprintf("%s #%u +data %s\n",tintstr(),id_,tosend.str().c_str());

    // RATELIMIT
    // ARNOSMPTODO: count overhead bytes too? Move to Send() then.
    transfer_->OnSendData(transfer()->chunk_size());

    return tosend;
}


void    Channel::AddAck (struct evbuffer *evb) {
    if (data_in_==tintbin())
    //if (data_in_.bin==bin64_t::NONE)
        return;
    // sometimes, we send a HAVE (e.g. in case the peer did repetitive send)
    evbuffer_add_8(evb, data_in_.time==TINT_NEVER?SWIFT_HAVE:SWIFT_ACK);
    evbuffer_add_chunkaddr(evb,data_in_.bin,hs_out_->chunk_addr_);
    // PPSPTODO LEDBAT one-way delay
    if (data_in_.time!=TINT_NEVER)
        evbuffer_add_64be(evb, data_in_.time);

    if (DEBUGTRAFFIC)
        fprintf(stderr,"send c%d: ACK %i\n", id(), bin_toUInt32(data_in_.bin));

    have_out_.set(data_in_.bin);
    dprintf("%s #%u +ack %s %lld\n",
        tintstr(),id_,data_in_.bin.str().c_str(),data_in_.time);
    if (data_in_.bin.layer()>2)
        data_in_dbl_ = data_in_.bin;

#if ENABLE_CANCEL == 1
    // Ric: check that we are not sending a cancel msg for data_in_
    std::deque<bin_t>::iterator it;
    bin_t b = data_in_.bin;
    for (it=cancel_out_.begin(); it!=cancel_out_.end(); it++) {
        bin_t c = *it;
        if (c == b) {
            cancel_out_.erase(it);
            break;
        }
        // b is always a single chunk :-)
        else if (c.contains(b)) {
            while (c.contains(b) && c!=b) {
                if (c>b) {
                    cancel_out_.insert(it+1,c.right());
                    c.to_left();
                }
                else {
                    cancel_out_.insert(it+1,c.left());
                    c.to_right();
                }
            }
            assert (c==b);
            cancel_out_.erase(it);
        }
    }
#endif
    data_in_ = tintbin();
    //data_in_ = tintbin(NOW,bin64_t::NONE);
}


void    Channel::AddHave (struct evbuffer *evb) {
    if (!data_in_dbl_.is_none()) { // TODO: do redundancy better
        evbuffer_add_8(evb, SWIFT_HAVE);
        evbuffer_add_chunkaddr(evb,data_in_dbl_,hs_out_->chunk_addr_);
        data_in_dbl_=bin_t::NONE;
    }
    if (DEBUGTRAFFIC)
        fprintf(stderr,"send c%d: HAVE ",id() );

    // ZEROSTATE
    if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
    {
        if (is_established())
            return;

        // Say we have peaks
        for(int i=0; i<hashtree()->peak_count(); i++) {
            bin_t peak = hashtree()->peak(i);
            evbuffer_add_8(evb, SWIFT_HAVE);
            evbuffer_add_chunkaddr(evb,peak,hs_out_->chunk_addr_);
            dprintf("%s #%u +have %s\n",tintstr(),id_,peak.str().c_str());
        }
        return;
    }

    for(int count=0; count<4; count++) {
        bin_t ack = binmap_t::find_complement(have_out_, *(transfer()->ack_out()), 0); // FIXME: do rotating queue
        if (ack.is_none())
            break;
        ack = transfer()->ack_out()->cover(ack);
        have_out_.set(ack);
        evbuffer_add_8(evb, SWIFT_HAVE);
        evbuffer_add_chunkaddr(evb,ack,hs_out_->chunk_addr_);

        if (DEBUGTRAFFIC)
            fprintf(stderr," %i", bin_toUInt32(ack));

        dprintf("%s #%u +have %s\n",tintstr(),id_,ack.str().c_str());
    }
    if (DEBUGTRAFFIC)
        fprintf(stderr,"\n");

}


void    Channel::Recv (struct evbuffer *evb) {
    dprintf("%s #%u recvd %ib\n",tintstr(),id_,(int)evbuffer_get_length(evb)+4);
    dgrams_rcvd_++;

    if (!transfer()->IsOperational()) {
    	dprintf("%s #%u recvd on broken transfer %d \n",tintstr(),id_, transfer()->td() );
	CloseOnError();
    	return;
    }

    lastrecvwaskeepalive_ = (evbuffer_get_length(evb) == 0);
    if (lastrecvwaskeepalive_)
        // Update speed measurements such that they decrease when DL stops
        transfer()->OnRecvData(0);

    if (last_send_time_ && rtt_avg_==TINT_SEC && dev_avg_==0) {
        rtt_avg_ = NOW - last_send_time_;
        dev_avg_ = rtt_avg_;
        dip_avg_ = rtt_avg_;
        dprintf("%s #%u sendctrl rtt init %lld\n",tintstr(),id_,rtt_avg_);
    }

    bin_t data = evbuffer_get_length(evb) ? bin_t::NONE : bin_t::ALL;

    if (DEBUGTRAFFIC)
        fprintf(stderr,"recv c%u: size " PRISIZET "\n", id(), evbuffer_get_length(evb));

    Handshake *hishs = NULL;
    if (hs_in_ == NULL) { // first reply from client
	if (hs_out_->version_ == VER_SWIFT_LEGACY) // I sent PPSPP_v1 HS, did not respond, now trying legacy
	    hishs = StaticOnHandshake(peer_,id(),true,VER_SWIFT_LEGACY,evb);
	else
	    hishs = StaticOnHandshake(peer_,id(),false,VER_PPSPP_v1,evb);
	if (hishs == NULL)
	    return;
	else
	    OnHandshake(hishs);
    }
    while (evbuffer_get_length(evb) && send_control_!=CLOSE_CONTROL) {
        uint8_t type = evbuffer_remove_8(evb);

        if (DEBUGTRAFFIC)
            fprintf(stderr," %d", type);

        switch (type) {
	    case SWIFT_HANDSHAKE: // explicit close
		hishs = StaticOnHandshake(peer_,id(),true,hs_in_->version_,evb);
		OnHandshake(hishs);
		break;
            case SWIFT_DATA:
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnDataZeroState(evb);
                else
                    data=OnData(evb);
                break;
            case SWIFT_HAVE:
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnHaveZeroState(evb);
                else
                    OnHave(evb);
                break;
            case SWIFT_ACK:
                OnAck(evb);
                break;
            case SWIFT_INTEGRITY:
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnHashZeroState(evb);
                else
                    OnHash(evb);
                break;
            case SWIFT_SIGNED_INTEGRITY: // PPSP
		OnSignedHash(evb);
                break;
            case SWIFT_REQUEST:
                OnHint(evb);
                break;
            case SWIFT_CANCEL: // PPSP
            	OnCancel(evb);
            	break;
            case SWIFT_PEX_RESv4:
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnPexAddZeroState(evb,AF_INET);
                else
                    OnPexAdd(evb,AF_INET);
                break;
            case SWIFT_PEX_RESv6: // PPSP
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnPexAddZeroState(evb,AF_INET6);
                else
                    OnPexAdd(evb,AF_INET6);
                break;
            case SWIFT_PEX_REScert: // PPSP
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnPexAddCertZeroState(evb);
                else
                    OnPexAddCert(evb);
                break;
            case SWIFT_PEX_REQ:
                if (transfer()->ttype() == FILE_TRANSFER && ((FileTransfer *)transfer())->IsZeroState())
                    OnPexReqZeroState(evb);
                else
                    OnPexReq();
                break;
            case SWIFT_CHOKE: // PPSP
		OnChoke(evb);
                break;
            case SWIFT_UNCHOKE: // PPSP
		OnUnchoke(evb);
                break;
            default:
                dprintf("%s #%u ?msg id unknown %i\n",tintstr(),id_,(int)type);
                return;
        }
    }
    if (DEBUGTRAFFIC)
    {
        fprintf(stderr,"\n");
    }

    // Arno, 2012-01-09: Provide PP with info needed for hook-in.
    if (transfer()->ttype() == LIVE_TRANSFER) {
        LiveTransfer *lt = (LiveTransfer *)transfer();
        if (!lt->am_source())
            ((LivePiecePicker *)lt->picker())->EndAddPeerPos(id() );
    }

    last_recv_time_ = NOW;
    sent_since_recv_ = 0;


    // Arno: see if transfer still in working order
    transfer()->UpdateOperational();
    if (!transfer()->IsOperational()) {
    	dprintf("%s #%u recvd broke transfer %d \n",tintstr(),id_, transfer()->td() );
        CloseOnError();
        return;
    }

    Reschedule();
}


void   Channel::CloseOnError()
{
    Close(CLOSE_SEND_IF_ESTABLISHED);
}


/*
 * Arno: FAXME: HASH+DATA should be handled as a transaction: only when the
 * hashes check out should they be stored in the hashtree, otherwise revert.
 */
void    Channel::OnHash (struct evbuffer *evb) {
    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0 || bv.size() > 1)
    {
	// chunk spec for hash must be power-of-2 range, so must fit in single bin
	dprintf("%s #%u ?hash bad chunk spec\n",tintstr(),id_);
	Close(CLOSE_DO_NOT_SEND);
	return;
    }
    bin_t pos = bv.front();
    Sha1Hash hash = evbuffer_remove_hash(evb);

    hashtree()->OfferHash(pos,hash);
    dprintf("%s #%u -hash %s\n",tintstr(),id_,pos.str().c_str());

    //fprintf(stderr,"HASH %lli hex %s\n",pos.toUInt(), hash.hex().c_str() );
}


void    Channel::CleanHintOut (bin_t pos) {
    int hi = 0;
    while (hi<hint_out_.size() && !hint_out_[hi].bin.contains(pos))
        hi++;
    if (hi==hint_out_.size())
        return; // something not hinted or hinted in far past
    while (hi--) { // removing likely snubbed hints
    	bin_t hint = hint_out_.front().bin;
        hint_out_size_ -= hint.base_length();
        hint_out_.pop_front();
#if ENABLE_CANCEL == 1
        // Ric: add to the cancel queue
        cancel_out_.push_back(hint);
#endif
    }
    while (hint_out_.front().bin!=pos) {
        tintbin f = hint_out_.front();

        assert (f.bin.contains(pos));

        if (pos < f.bin) {
            f.bin.to_left();
        } else {
            f.bin.to_right();
        }

        hint_out_.front().bin = f.bin.sibling();
        hint_out_.push_front(f);
    }
#if ENABLE_CANCEL == 1
    // Ric: add to the cancel queue
    cancel_out_.push_back(hint_out_.front().bin);
#endif
    hint_out_size_ -= hint_out_.front().bin.base_length();
    hint_out_.pop_front();
}


bin_t Channel::OnData (struct evbuffer *evb) {  // TODO: HAVE NONE for corrupted data

    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0 || bv.size() > 1) {
		// Chunk spec must denote single chunk
		dprintf("%s #%u ?data bad chunk spec\n",tintstr(),id_);
		Close(CLOSE_DO_NOT_SEND);
		return bin_t::NONE;
    }
    bin_t pos = bv.front();
    tint peer_time = TINT_NEVER;
    if (hs_out_->version_ == VER_PPSPP_v1)
    	peer_time = evbuffer_remove_64be(evb);

    // Arno: Assuming DATA last message in datagram
    if (evbuffer_get_length(evb) > transfer()->chunk_size()) {
    	dprintf("%s #%u !data chunk size mismatch %s: exp %u got " PRISIZET "\n",tintstr(),id_,pos.str().c_str(), transfer()->chunk_size(), evbuffer_get_length(evb));
    	fprintf(stderr,"WARNING: chunk size mismatch: exp %lu got " PRISIZET "\n",transfer()->chunk_size(), evbuffer_get_length(evb));
    }

    int length = (evbuffer_get_length(evb) < transfer()->chunk_size()) ? evbuffer_get_length(evb) : transfer()->chunk_size();
    if (!transfer()->ack_out()->is_empty(pos)) {
        // Arno, 2012-01-24: print message for duplicate
        dprintf("%s #%u Ddata %s\n",tintstr(),id_,pos.str().c_str());
        evbuffer_drain(evb, length);
        data_in_ = tintbin(TINT_NEVER,transfer()->ack_out()->cover(pos));

        // Arno, 2012-01-24: Make sure data interarrival periods don't get
        // screwed up because of these (ignored) duplicates.
        UpdateDIP(pos);
        return bin_t::NONE;
    }

    uint8_t *data = evbuffer_pullup(evb, length);

    //fprintf(stderr,"OnData: Got chunk %d / %lli\n", length, swift::SeqComplete(transfer()->fd()) );

    //LIVE
    if (transfer()->ttype() == FILE_TRANSFER) {
        if (!hashtree()->OfferData(pos, (char*)data, length)) {
            evbuffer_drain(evb, length);
            dprintf("%s #%u !data %s\n",tintstr(),id_,pos.str().c_str());
            return bin_t::NONE;
        }

        // Arno: If we are getting content, keep activated
        swift::Touch(transfer()->td());
    }
    else
    {
        //LIVE: actually write data to storage
        // LIVETODO: content integrity checking
        int ret = transfer()->GetStorage()->Write(data,length,pos.base_offset()*transfer()->chunk_size());
        if (ret < 0)
        {
            print_error("storage Write failed");
                exit(-1);
        }
        else
            transfer()->ack_out()->set(pos);
    }

    evbuffer_drain(evb, length);
    dprintf("%s #%u -data %s\n",tintstr(),id_,pos.str().c_str());

    if (DEBUGTRAFFIC)
        fprintf(stderr,"$ ");


    bin_t cover = transfer()->ack_out()->cover(pos);
    transfer()->Progress(cover);
    //if (cover.layer() >= 5) // Arno: update DL speed. Tested with 32K, presently = 2 ** 5 * chunk_size CHUNKSIZE
    //    transfer()->OnRecvData( pow((double)2,(double)5)*((double)transfer()->chunk_size()) );
    transfer()->OnRecvData( transfer()->chunk_size() );

    data_in_ = tintbin(NOW,bin_t::NONE);
    data_in_.bin = pos;
    // Ric: the time of the ack is the owd.
	if (peer_time!=TINT_NEVER)
		data_in_.time = NOW - peer_time;

    UpdateDIP(pos);
    CleanHintOut(pos);
    bytes_down_ += length;
    global_bytes_down += length;
    return pos;
}


void Channel::UpdateDIP(bin_t pos)
{
    if (!pos.is_none()) {
        if (last_data_in_time_) {
            tint dip = NOW - last_data_in_time_;
            dip_avg_ = ( dip_avg_*3 + dip ) >> 2;
        }
        last_data_in_time_ = NOW;
    }
}


void    Channel::OnAck (struct evbuffer *evb) {

    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0) {
	// Could not parse chunk spec
	dprintf("%s #%u ?ack bad chunk spec\n",tintstr(),id_);
	Close(CLOSE_DO_NOT_SEND);
	return;
    }
    tint peer_owd = evbuffer_remove_64be(evb);

    binvector::iterator iter;
    for (iter=bv.begin(); iter != bv.end(); iter++)
    {
	bin_t ackd_pos = *iter;

	// FIXME FIXME: wrap around here
	if (ackd_pos.is_none()) // safety catch
	    return; // likely, broken chunk/ insufficient hashes
	//LIVE
	if (transfer()->ttype() == FILE_TRANSFER && hashtree()->size() && ackd_pos.base_offset()>=hashtree()->size_in_chunks()) {
	    eprintf("invalid ack: %s\n",ackd_pos.str().c_str());
	    return;
	}
	ack_in_.set(ackd_pos);

	//fprintf(stderr,"OnAck: got bin %s is_complete %d\n", ackd_pos.str(), (int)ack_in_.is_complete_arno( transfer()->ack_out()->get_height() ));

	int di = 0, ri = 0;
	// find an entry for the send (data out) event
	while (di<data_out_.size() && (data_out_[di]==tintbin() || !ackd_pos.contains(data_out_[di].bin))  )
	    di++;
	// FUTURE: delayed acks
	// rule out retransmits
	while (ri<data_out_tmo_.size() && !ackd_pos.contains(data_out_tmo_[ri].bin) )
	    ri++;
	dprintf("%s #%u %cack %s owd:%lld\n",tintstr(),id_,
		di==data_out_.size()?'?':'-',ackd_pos.str().c_str(),peer_owd);
	if (di!=data_out_.size() && ri==data_out_tmo_.size()) { // not a retransmit
	    // round trip time calculations
		// Ric: TODO delayed acks
	    tint rtt = NOW-data_out_[di].time;
	    rtt_avg_ = (rtt_avg_*7 + rtt) >> 3;
	    dev_avg_ = ( dev_avg_*3 + tintabs(rtt-rtt_avg_) ) >> 2;
	    assert(data_out_[di].time!=TINT_NEVER);
	    // one-way delay calculations
	    // Ric: we r always re-writing the first element
	    owd_cur_bin_ = 0;//(owd_cur_bin_+1) & 3;
	    owd_current_[owd_cur_bin_] = peer_owd;
	    if ( owd_min_bin_start_+TINT_SEC*30 < NOW ) {
			owd_min_bin_start_ = NOW;
			owd_min_bin_ = (owd_min_bin_+1) & 3;
			owd_min_bins_[owd_min_bin_] = TINT_NEVER;
	    }
	    if (owd_min_bins_[owd_min_bin_]>peer_owd)
	    	owd_min_bins_[owd_min_bin_] = peer_owd;
	    // Arno, 2012-12-20: Temp disable, getting SEGV on this
	    // dprintf("%s #%u sendctrl rtt %ld dev %ld based on %s\n",
	    //	    tintstr(),id_,rtt_avg_,dev_avg_,data_out_[di].bin.str().c_str());
	    ack_rcvd_recent_++;
	    // early loss detection by packet reordering
	    for (int re=0; re<di-MAX_REORDERING; re++) {
			if (data_out_[re]==tintbin())
				continue;
			ack_not_rcvd_recent_++;
			data_out_tmo_.push_back(data_out_[re].bin);
			dprintf("%s #%u Rdata %s\n",tintstr(),id_,data_out_.front().bin.str().c_str());
			data_out_cap_ = bin_t::ALL;
			data_out_[re] = tintbin();
	    }
	}
	if (di!=data_out_.size())
	    data_out_[di]=tintbin();
    }

    // clear zeroed items
    while (!data_out_.empty() && ( data_out_.front()==tintbin() ||
            ack_in_.is_filled(data_out_.front().bin) ) )
        data_out_.pop_front();
    assert(data_out_.empty() || data_out_.front().time!=TINT_NEVER);
}


void Channel::TimeoutDataOut ( ) {
    // losses: timeouted packets
    tint timeout = NOW - ack_timeout();
    while (!data_out_.empty() &&
        ( data_out_.front().time<timeout || data_out_.front()==tintbin() ) ) {
        if (data_out_.front()!=tintbin() && ack_in_.is_empty(data_out_.front().bin)) {
            ack_not_rcvd_recent_++;
            data_out_cap_ = bin_t::ALL;
            data_out_tmo_.push_back(data_out_.front().bin);
            dprintf("%s #%u Tdata %s\n",tintstr(),id_,data_out_.front().bin.str().c_str());
        }
        data_out_.pop_front();
    }
    // clear retransmit queue of older items
    while (!data_out_tmo_.empty() && data_out_tmo_.front().time<NOW-MAX_POSSIBLE_RTT)
        data_out_tmo_.pop_front();
}


void Channel::OnHave (struct evbuffer *evb) {

    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0) {
	// Could not parse chunk spec
	dprintf("%s #%u ?have bad chunk spec\n",tintstr(),id_);
	Close(CLOSE_DO_NOT_SEND);
	return;
    }
    binvector::iterator iter;
    for (iter=bv.begin(); iter != bv.end(); iter++)
    {
	bin_t ackd_pos = *iter;

	if (ackd_pos.is_none()) // safety catch
	    return; // wow, peer has hashes

	// PPPLUG
	if (ENABLE_VOD_PIECEPICKER && transfer()->ttype() == FILE_TRANSFER) {
	    FileTransfer *ft = (FileTransfer *)transfer();
	    // Ric: check if we should set the size in the file transfer
	    if (ft->availability()->size() <= 0 && ft->hashtree()->size() > 0)
	    {
		ft->availability()->setSize(hashtree()->size_in_chunks());
	    }
	    // Ric: update the availability if needed
	    ft->availability()->set(id_, ack_in_, ackd_pos);
	}

	ack_in_.set(ackd_pos);
	dprintf("%s #%u -have %s\n",tintstr(),id_,ackd_pos.str().c_str());

	// Arno, 2012-01-09: Provide PP with info needed for hook-in.
	// Arno, 2013-02-14: Could be optimized such that only right-most bin
	// is communicated to LivePiecePicker.
	if (transfer()->ttype() == LIVE_TRANSFER)
	{
	    LiveTransfer *lt = (LiveTransfer *)transfer();
	    if (!lt->am_source())
	    {
		((LivePiecePicker *)lt->picker())->StartAddPeerPos(id(), ackd_pos.base_right(), peer_is_source_);

		// Arno: it can happen that we receive a HAVE and have no hints
		// outstanding. In that case we should not wait till next_send_time_
		// but request directly. See send_control.cpp
		if (hint_out_.size() == 0)
		{
		    live_have_no_hint_ = true;
		    dprintf("%s #%u have but no hints\n",tintstr(),id_);
		}
	    }
	}
    }
}


void    Channel::OnHint (struct evbuffer *evb) {

    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0) {
	// Could not parse chunk spec
	dprintf("%s #%u ?hint bad chunk spec\n",tintstr(),id_);
	Close(CLOSE_DO_NOT_SEND);
	return;
    }

    binvector::iterator iter;
    for (iter=bv.begin(); iter != bv.end(); iter++)
    {
	bin_t hint = *iter;

	// FIXME: wake up here
	hint_in_.push_back(hint);
	hint_in_size_ += hint.base_length();
	dprintf("%s #%u -hint %s [%llu]\n",tintstr(),id_,hint.str().c_str(),hint_in_size_);
    }
}

/*
 * Read full HANDSHAKE message from evb outside of a Channel context.
 */
Handshake *Channel::StaticOnHandshake( Address &addr, uint32_t cid, bool ver_known, popt_version_t ver, struct evbuffer *evb)
{
    dprintf("StaticOnHandshake: %s id %u known %d ver %d\n", addr.str().c_str(), cid, (int)ver_known, ver );

    Handshake *hs = new Handshake();

    if (!ver_known)
    {
	uint8_t msgid = evbuffer_remove_8(evb);
	if (msgid == SWIFT_INTEGRITY)
	    ver = VER_SWIFT_LEGACY;
	else if (msgid == SWIFT_HANDSHAKE)
	    ver = VER_PPSPP_v1;
	else
	{
	    dprintf("%s #%u ?hs unknown protocol %d %s\n", tintstr(),cid,msgid,addr.str().c_str());
	    delete hs;
	    return NULL;
	}
    }

    if (ver == VER_SWIFT_LEGACY)
    {
	//dprintf("%s #%u -hs swift legacy\n", tintstr(), cid );
	hs->version_ = VER_SWIFT_LEGACY;
	if (cid == 0)
	{
	    // He is initiating. Initiating handshake has SWIFT_HASH + root hash, reply doesn't
	    if (evbuffer_get_length(evb)<4+1+4+Sha1Hash::SIZE)
	    {
	        dprintf("%s #0 incorrect size %i initial handshake packet %s\n", tintstr(),(int)evbuffer_get_length(evb),addr.str().c_str());
	        delete hs;
	        return NULL;
	    }
	    bin_t pos = bin_fromUInt32(evbuffer_remove_32be(evb));
	    if (!pos.is_all()) {
		dprintf("%s #%u ?hs that is not the root hash %s\n",tintstr(), cid, addr.str().c_str());
	       delete hs;
	       return NULL;
	    }
	    Sha1Hash swarmid = evbuffer_remove_hash(evb);
	    hs->SetSwarmID(swarmid);
	    dprintf("%s #%u -hash ALL %s\n",tintstr(),cid,swarmid.hex().c_str());
	}

	// Read SWIFT_HANDSHAKE
	uint8_t msgid = evbuffer_remove_8(evb);
	hs->peer_channel_id_ = evbuffer_remove_32be(evb);
	hs->ResetToLegacy();

	dprintf("%s #%u -hs swift %x\n",tintstr(),cid,hs->peer_channel_id_);
    }
    else if (ver == VER_PPSPP_v1)
    {
	// IETF PPSP compliant
	//dprintf("%s #%u -hs ietf ppsp\n", tintstr(),cid );
	hs->peer_channel_id_ = evbuffer_remove_32be(evb);
	bool end=false;
	uint8_t size8 = 0, i8=0;
	uint16_t size = 0;
	uint8_t *swarmidbytes = NULL;
	uint8_t *msgbitmapbytes = NULL;
	Sha1Hash swarmid;
	std::ostringstream cross;
	while (!end && evbuffer_get_length(evb) > 0)
	{
	    popt_t poid = (popt_t)evbuffer_remove_8(evb);
	    //dprintf("%s #%u -hs popt %d\n", tintstr(),cid, (int)poid );
	    switch (poid) {
		case POPT_VERSION:
		    hs->version_ = (popt_version_t)evbuffer_remove_8(evb);
		    cross << "v" << hs->version_ << " ";
		    break;
		case POPT_MIN_VERSION:
		    hs->min_version_ = (popt_version_t)evbuffer_remove_8(evb);
		    cross << "nv" << hs->min_version_ << " ";
		    break;
		case POPT_SWARMID:
		    size = evbuffer_remove_16be(evb);
		    if (size > POPT_MAX_SWARMID_SIZE || evbuffer_get_length(evb) < size) {
			dprintf("%s #%u ?hs popt swarmid too big\n",tintstr(),cid);
			delete hs;
			return NULL;
		    }
		    swarmidbytes = evbuffer_pullup(evb,size);
		    swarmid = Sha1Hash(false,(const char *)swarmidbytes);
		    hs->SetSwarmID(swarmid);
		    cross << "sid " << swarmid.hex() << " ";
		    evbuffer_drain(evb, size);
		    break;
		case POPT_CONT_INT_PROT:
		    hs->cont_int_prot_ = (popt_cont_int_prot_t)evbuffer_remove_8(evb);
		    cross << "cipm " << hs->cont_int_prot_ << " ";
		    break;
		case POPT_MERKLE_HASH_FUNC:
		    hs->merkle_func_ = (popt_merkle_func_t)evbuffer_remove_8(evb);
		    cross << "mhf " << hs->merkle_func_ << " ";
		    break;
		case POPT_LIVE_SIG_ALG:
		    hs->live_sig_alg_ = (popt_live_sig_alg_t)evbuffer_remove_8(evb);
		    cross << "lsa " << hs->live_sig_alg_ << " ";
		    break;
		case POPT_CHUNK_ADDR:
		    hs->chunk_addr_ = (popt_chunk_addr_t)evbuffer_remove_8(evb);
		    cross << "cam " << hs->chunk_addr_ << " ";
		    break;
		case POPT_LIVE_DISC_WND:
		    if (hs->chunk_addr_ == POPT_CHUNK_ADDR_BIN32 || hs->chunk_addr_ == POPT_CHUNK_ADDR_CHUNK32)
			hs->live_disc_wnd_ = evbuffer_remove_32be(evb);
		    else
			hs->live_disc_wnd_ = evbuffer_remove_64be(evb);
		    cross << "ldw " << std::hex << hs->live_disc_wnd_ << std::dec << " ";
		    break;
		case POPT_SUPP_MSGS:
		    size8 = evbuffer_remove_8(evb);
		    if (size8 > 8 || evbuffer_get_length(evb) < size8) {
			dprintf("%s #%u ?hs popt supp msgs too big\n",tintstr(),cid);
			delete hs;
			return NULL;
		    }
		    msgbitmapbytes = evbuffer_pullup(evb,size8);
		    evbuffer_drain(evb, size);
		    cross << "msgs " << std::hex;
		    for (i8=0; i8<size8; i8++)
			cross << (int)msgbitmapbytes[i8];
		    cross << std::dec;
		    break;
		case POPT_END:
		    end = true;
		    dprintf("%s #%u -hs %x ppsp %s\n", tintstr(), cid, hs->peer_channel_id_, cross.str().c_str() );
		    break;
		default:
		    dprintf("%s #%u ?hs popt id unknown %i\n",tintstr(),cid,(int)poid);
		    delete hs;
		    return NULL;
	    }
	}
    }

    return hs;
}

void Channel::OnHandshake(Handshake *hishs) {

    dprintf("OnHandshake\n");

    if (hishs->peer_channel_id_ == 0) {
        // Arno: Got explicit close from peer, close channel and don't send reply
	dprintf("%s #%u -hs close\n",tintstr(),id_);
        Close(CLOSE_DO_NOT_SEND);
        delete hishs;
        return;
    }
    else
	dprintf("%s #%u -hs %x %s opened as channel %u\n",tintstr(),id_,hishs->peer_channel_id_,(hishs->version_ == VER_SWIFT_LEGACY) ? "swift" : "ppsp", id_);

    if (!hishs->IsSupported())
    {
	dprintf("%s #%u -hs unsupported\n",tintstr(),id_);
	Close(CLOSE_SEND);
	delete hishs;
	return;
    }

    hs_in_ = hishs;
    hs_in_->ReleaseSwarmID(); // save mem per channel

    // Self-connection check
    if (!SELF_CONN_OK) {
        uint32_t try_id = DecodeID(hishs->peer_channel_id_);
        // Arno, 2012-05-29: Fixed duplicate test
        if (channel(try_id) == this) {
            // this is a self-connection
            dprintf("%s #%u -hs closing self\n",tintstr(),id_);
            Close(CLOSE_SEND);
            hs_in_ = NULL;
            delete hishs;
            return;
        }
    }

    if (hs_in_->version_ == VER_SWIFT_LEGACY)
	hs_out_->ResetToLegacy(); // he speaks legacy, so will I

    // FUTURE: channel forking
    if (is_established()) // when this was reply to our HS
        dprintf("%s #%u established %s\n", tintstr(), id_, peer().str().c_str());
}


void    Channel::OnCancel (struct evbuffer *evb) {

    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0) {
	// Could not parse chunk spec
	dprintf("%s #%u ?cancel bad chunk spec\n",tintstr(),id_);
	Close(CLOSE_DO_NOT_SEND);
	return;
    }

    // Arno, 2012-11-23: chunkaddr translated to list of bins, iterate and
    // process in two ways:
    // 1. Remove bins from hint_in_ that are contained in the cancelled bins.
    //    A hint may have been partially answered so it may be split into
    //    smaller bins.
    // 2. Split up bins from hint_in_ that have only been partially canceled.
    //    i.e., where cancelbin is contained by a bin in hint_in_
    //
    // If the hint is already in progress (i.e, already transmitted, not yet
    // acked), we let it be.
    //
    binvector::iterator iter;
    for (iter=bv.begin(); iter != bv.end(); iter++)
    {
	bin_t cancelbin = *iter;
	dprintf("%s #%u -cancel %s\n",tintstr(),id_,cancelbin.str().c_str());

	// 1. Remove hint from hint_in_ if contained in cancelbin. Use Riccardo's solution:
	int hi = 0;
	while (hi<hint_in_.size() && !cancelbin.contains(hint_in_[hi].bin) && cancelbin != hint_in_[hi].bin)
	    hi++;

	// something to cancel?
	if (hi != hint_in_.size())
	{
	    // Assumption: all fragments of a bin being cancelled consecutive in hint_in_
	    do {
		//dprintf("%s #%u -cancel frag %s\n",tintstr(),id_,hint_in_[hi].bin.str().c_str());
	    	hint_in_size_ -= hint_in_[hi].bin.base_length();
			hint_in_.erase(hint_in_.begin()+hi);
			if (hint_in_.size() == 0 || hi >= hint_in_.size())
				break;
	    } while (cancelbin.contains(hint_in_[hi].bin));
	}

	//dprintf("%s #%u -cancel ORIG %s len %d\n",tintstr(),id_,cancelbin.str().c_str(), hint_in_.size() );

	// 2. Fragment hint from hint_in_ if it covers cancelbin. Use Riccardo's solution:
	hi = 0;
	while (hi<hint_in_.size() && !hint_in_[hi].bin.contains(cancelbin))
	    hi++;

	// nothing to cancel
	if (hi==hint_in_.size())
	    continue;

	// Split up hint
	tint origt = hint_in_[hi].time;
	bin_t origbin = hint_in_[hi].bin;
	binvector fragbins = swift::bin_fragment(origbin,cancelbin);
	// Erase original
	hint_in_size_ -= hint_in_[hi].bin.base_length();
	hint_in_.erase(hint_in_.begin()+hi);
	// Replace with fragments left
	binvector::iterator iter2;
	int idx=0;
	for (iter2=fragbins.begin(); iter2 != fragbins.end(); iter2++)
	{
	    bin_t fragbin = *iter2;
	    //dprintf("%s #%u -cancel keep %s\n",tintstr(),id_,fragbin.str().c_str());
	    tintbin newtb(origt,fragbin);
	    hint_in_size_ += fragbin.base_length();
	    hint_in_.insert(hint_in_.begin()+hi+idx,newtb);
	    idx++;
	}
    }

    for (int i=0; i<hint_in_.size(); i++)
    {
	dprintf("%s #%u -cancel NETS %s\n",tintstr(),id_,hint_in_[i].bin.str().c_str());
    }
}


void Channel::OnPexAdd(struct evbuffer *evb, int family) {
    Address addr = evbuffer_remove_pexaddr(evb,family);
    dprintf("%s #%u -pex %s %s\n",tintstr(),id_,(addr.get_family() == AF_INET) ? "v4" : "v6", addr.str().c_str() );

    if (transfer()->OnPexIn(addr))
        useless_pex_count_ = 0;
    else
    {
        dprintf("%s #%u already channel to %s\n", tintstr(),id_,addr.str().c_str());
        useless_pex_count_++;
    }
    pex_request_outstanding_ = false;
}


void Channel::OnPexAddCert(struct evbuffer *evb)
{
    OnPexAddCertZeroState(evb);
    dprintf("%s #%u -pex cert\n",tintstr(),id_);
}


void Channel::OnChoke(struct evbuffer *evb)
{
    if (hs_in_->version_ == VER_SWIFT_LEGACY) { // FRAGRAND support
	evbuffer_remove_32be(evb); // read 4 random bytes
	return;
    }

    //PPSPTODO
    dprintf("%s #%u -choke\n",tintstr(),id_);
}

void Channel::OnUnchoke(struct evbuffer *evb)
{
    //PPSPTODO
    dprintf("%s #%u -unchoke\n",tintstr(),id_);
}


void Channel::OnSignedHash(struct evbuffer *evb)
{
    binvector bv = evbuffer_remove_chunkaddr(evb,hs_in_->chunk_addr_);
    if (bv.size() == 0 || bv.size() > 1)
    {
    	// chunk spec for hash must be power-of-2 range, so must fit in single bin
    	dprintf("%s #%u ?sighash bad chunk spec\n",tintstr(),id_);
    	Close(CLOSE_DO_NOT_SEND);
    	return;
    }
    bin_t pos = bv.front();

    // PPSPTODO
    //if (hs_in_->live_sig_alg_ == POPT_LIVE_SIG_ALG_PRIVATEDNS)
    //evbuffer_drain(evb, size);
}


/*
 * Sending messages
 */

void    Channel::AddPex (struct evbuffer *evb) {
    // Gertjan fix: Reverse PEX
    // PEX messages sent to facilitate NAT/FW puncturing get priority
    if (!reverse_pex_out_.empty()) {
        do {
            tintbin pex_peer = reverse_pex_out_.front();
            reverse_pex_out_.pop_front();
            if (channels[(int) pex_peer.bin.toUInt()] == NULL)
                continue;
            Address a = channels[(int) pex_peer.bin.toUInt()]->peer();
            // Arno, 2012-02-28: Don't send private addresses to non-private peers.
            if (!a.is_private() || (a.is_private() && peer().is_private()))
            {
        	evbuffer_add_pexaddr(evb, a);
        	dprintf("%s #%u +pex (reverse) %s\n",tintstr(),id_,a.str().c_str());
            }
        } while (!reverse_pex_out_.empty() && (SWIFT_MAX_NONDATA_DGRAM_SIZE-evbuffer_get_length(evb)) >= 7);

        // Arno: 2012-02-23: Don't think this is right. Bit of DoS thing,
        // that you only get back the addr of people that got your addr.
        // Disable for now.
        //return;
    }

    if (!pex_requested_)
        return;

    // Arno, 2012-02-28: Don't send private addresses to non-private peers.
    int tries=0;
    Channel *c=NULL;
    Address a;
    while (true)
    {
        // Arno, 2011-10-03: Choosing Gertjan's RandomChannel over RevealChannel here.
        c = transfer()->RandomChannel(this);
        if (c == NULL || tries > 5) {
            pex_requested_ = false;
            return;
        }
        a = c->peer();
        if (!a.is_private() || (a.is_private() && peer().is_private()))
            break;
        tries++;
    }

    evbuffer_add_pexaddr(evb, a);
    dprintf("%s #%u +pex %s\n",tintstr(),id_,a.str().c_str());

    pex_requested_ = false;
    /* Ensure that we don't add the same id to the reverse_pex_out_ queue
       more than once. */
    int chid = c->id();
    for (tbqueue::iterator i = channels[chid]->reverse_pex_out_.begin();
            i != channels[chid]->reverse_pex_out_.end(); i++)
        if ((int) (i->bin.toUInt()) == id_)
            return;

    dprintf("%s #%u adding pex for channel %u at time %s\n", tintstr(), chid,
        id_, tintstr(NOW + 2 * TINT_SEC));
    // Arno, 2011-10-03: should really be a queue of (tint,channel id(= uint32_t)) pairs.
    channels[chid]->reverse_pex_out_.push_back(tintbin(NOW + 2 * TINT_SEC, bin_t(id_)));
    if (channels[chid]->send_control_ == KEEP_ALIVE_CONTROL &&
            channels[chid]->next_send_time_ > NOW + 2 * TINT_SEC)
        channels[chid]->Reschedule();
}



void Channel::OnPexReq(void) {
    dprintf("%s #%u -pex req\n", tintstr(), id_);
    if (NOW > MIN_PEX_REQUEST_INTERVAL + last_pex_request_time_)
        pex_requested_ = true;
}

void Channel::AddPexReq(struct evbuffer *evb) {
    // Rate limit the number of PEX requests
    if (NOW < next_pex_request_time_)
        return;

    // If no answer has been received from a previous request, count it as useless
    if (pex_request_outstanding_)
        useless_pex_count_++;

    pex_request_outstanding_ = false;

    // Initiate at most SWIFT_MAX_CONNECTIONS connections
    if (transfer()->GetChannels()->size() >= SWIFT_MAX_CONNECTIONS ||
            // Check whether this channel has been providing useful peer information
            useless_pex_count_ > 2)
    {
        // Arno, 2012-02-23: Fix: Code doesn't recover from useless_pex_count_ > 2,
        // let's just try again in 30s
        useless_pex_count_ = 0;
        next_pex_request_time_ = NOW + 30 * TINT_SEC;

        return;
    }

    dprintf("%s #%u +pex req\n", tintstr(), id_);
    evbuffer_add_8(evb, SWIFT_PEX_REQ);
    /* Add a little more than the minimum interval, such that the other party is
       less likely to drop it due to too high rate */
    next_pex_request_time_ = NOW + MIN_PEX_REQUEST_INTERVAL * 1.1;
    pex_request_outstanding_ = true;
}



/*
 * Channel class methods
 */

void Channel::LibeventReceiveCallback(evutil_socket_t fd, short event, void *arg) {
    // Called by libevent when a datagram is received on the socket
    Time();
    dprintf("%s recv callback\n",tintstr() );

    RecvDatagram(fd);
    event_add(&evrecv, NULL);
}

void    Channel::RecvDatagram (evutil_socket_t socket) {
    struct evbuffer *evb = evbuffer_new();
    Address addr;
    Handshake *hishs = NULL;

    RecvFrom(socket, addr, evb);
    size_t evboriglen = evbuffer_get_length(evb);

    dprintf("%s recvdgram " PRISIZET "\n",tintstr(),evboriglen );

//#define return_log(...) { fprintf(stderr,__VA_ARGS__); evbuffer_free(evb); return; }
#define return_log(...) { dprintf(__VA_ARGS__); evbuffer_free(evb); if (hishs != NULL) { delete hishs; } return; }
    if (evbuffer_get_length(evb)<4)
        return_log("socket layer weird: datagram < 4 bytes from %s (prob ICMP unreach)\n",addr.str().c_str());
    uint32_t mych = evbuffer_remove_32be(evb);
    Sha1Hash hash;
    Channel* channel = NULL;
    if (mych==0) { // peer initiates handshake

	hishs = StaticOnHandshake(addr,0,false,VER_PPSPP_v1,evb);
	if (hishs == NULL) // dprintf already called
	    return_log ("%s #0 ?hs bad\n",tintstr());

        int td = swift::Find(hishs->GetSwarmID(),true); // Activate
        if (td < 0)
        {
            // No known swarm, check if available as zero state
            ZeroState *zs = ZeroState::GetInstance();
            td = zs->Find(hishs->GetSwarmID());
            if (td == -1)
                return_log ("%s #0 hash %s unknown, requested by %s\n",tintstr(),hishs->GetSwarmID().hex().c_str(),addr.str().c_str());
        }
        ContentTransfer *ct = swift::GetActivatedTransfer(td);
        if (ct == NULL)
        {
	    return_log( "%s #0 hash %s known, couldn't be activated; requested by %s\n", tintstr(), hishs->GetSwarmID().hex().c_str(), addr.str().c_str() );
        }
        else if (!ct->IsOperational())
        {
            // Activated, but broken
            return_log ("%s #0 hash %s broken, requested by %s\n",tintstr(),hishs->GetSwarmID().hex().c_str(),addr.str().c_str());
        }

        // Arno, 2012-02-27: Check for duplicate channel
        Channel* existchannel = ct->FindChannel(addr,NULL);
        if (existchannel != NULL)
        {
            // Arno: 2011-10-13: Ignore if established, otherwise consider
            // it a concurrent connection attempt.
            if (existchannel->is_established()) {
                // ARNOTODO: Read complete handshake here so we know whether
                // attempt is to new channel or to existing. Currently read
                // in OnHandshake()
                //
                return_log("%s #0 have a channel already to %s\n",tintstr(),addr.str().c_str());
            } else {
                channel = existchannel;
                //fprintf(stderr,"Channel::RecvDatagram: HANDSHAKE: reuse channel %s\n", channel->peer_.str().c_str() );
            }
        }
        if (channel == NULL) {
            //fprintf(stderr,"Channel::RecvDatagram: HANDSHAKE: create new channel %s\n", addr.str().c_str() );
            channel = new Channel(ct, socket, addr);
        }
        //fprintf(stderr,"CHANNEL INCOMING DEF hass %s is id %d\n",hishs->GetSwarmID().hex().c_str(),channel->id());

        channel->OnHandshake(hishs);

    } else if (mych==CMDGW_TUNNEL_DEFAULT_CHANNEL_ID) {
        // SOCKTUNNEL
        CmdGwTunnelUDPDataCameIn(addr,CMDGW_TUNNEL_DEFAULT_CHANNEL_ID,evb);
        evbuffer_free(evb);
        return;
    } else { // peer responds to my handshake (and other messages)
        mych = DecodeID(mych);
        if (mych>=channels.size())
            return_log("%s invalid channel #%u, %s\n",tintstr(),mych,addr.str().c_str());
        channel = channels[mych];
        if (!channel)
            return_log ("%s #%u is already closed\n",tintstr(),mych);
        if (channel->IsDiffSenderOrDuplicate(addr,mych)) {
            dprintf("%s #%u ?channel diff or dup\n",tintstr(),mych);
            channel->Close(CLOSE_SEND_IF_ESTABLISHED);
            delete channel; // safe, not in a channel event
            return_log ("%s #%u is duplicate\n",tintstr(),mych);
        }
        channel->own_id_mentioned_ = true;
    }
    channel->raw_bytes_down_ += evboriglen;
    //dprintf("recvd %i bytes for %i\n",data.size(),channel->id);
    bool wasestablished = channel->is_established();

    //dprintf("%s #%u peer %s recv_peer %s addr %s\n", tintstr(),mych, channel->peer().str().c_str(), channel->recv_peer().str(), addr.str() );

    // Process messages
    if (channel->send_control_!=CLOSE_CONTROL)
	channel->Recv(evb);

    evbuffer_free(evb);

    //SAFECLOSE
    if (channel->send_control_==CLOSE_CONTROL) {
        // Arno, 2012-07-27: Received an explict close, clean up channel
        delete channel;
    }
}



/*
 * Channel instance methods
 */

void Channel::CloseChannelByAddress(const Address &addr)
{
    // fprintf(stderr,"CloseChannelByAddress: address is %s\n", addr.str().c_str() );

    dprintf("%s #-1 close channel by address %s\n",tintstr(), addr.str().c_str() );
    channels_t::iterator iter;
    for (iter = channels.begin(); iter != channels.end(); iter++)
    {
        Channel *c = *iter;
        if (c != NULL && c->peer_ == addr) {
            dprintf("%s #%u close by addr\n",tintstr(),c->id());
            c->Close(CLOSE_DO_NOT_SEND);
            delete c; // safe, not in a send event, and doesn't modify channels
            break;
        }
    }
}

void Channel::Close(close_send_t closesend) {

    dprintf("%s #%u closing channel\n",tintstr(),id_ );

    this->SwitchSendControl(CLOSE_CONTROL);

    if (closesend == CLOSE_SEND || (closesend == CLOSE_SEND_IF_ESTABLISHED && is_established()))
        this->Send(); // Arno: send explicit close

    if (hs_in_ != NULL) // is_established() -> false
	hs_in_->peer_channel_id_ = 0;

    //LIVE
    if (ENABLE_VOD_PIECEPICKER && transfer()->ttype() == FILE_TRANSFER) {
        FileTransfer *ft = (FileTransfer *)transfer();
        if (!ft->IsZeroState() && ft->availability() != NULL) // availability() is NULL when this is called from ContentTransfer/CloseChannels()
        {
            // Ric: remove its binmap from the availability
            ft->availability()->remove(id_, ack_in_);
        }
    }

    // SAFECLOSE
    // Arno: ensure LibeventSendCallback is no longer called with ptr to this Channel
    ClearEvents();
}



void Channel::Reschedule () {

    // Arno: CAREFUL: direct send depends on diff between next_send_time_ and
    // NOW to be 0, so any calls to Time in between may put things off. Sigh.
    Time();
    dprintf("%s schedule\n",tintstr() );

    next_send_time_ = NextSendTime();
    if (next_send_time_!=TINT_NEVER) {

        assert(next_send_time_<NOW+TINT_MIN);
        tint duein = next_send_time_-NOW;
        if (duein <= 0 && !direct_sending_) {
            // Arno, 2011-10-18: libevent's timer implementation appears to be
            // really slow, i.e., timers set for 100 usec from now get called
            // at least two times later :-( Hence, for sends after receives
            // perform them directly.
            dprintf("%s #%u requeue direct send\n",tintstr(),id_);
            direct_sending_ = true;
            LibeventSendCallback(-1,EV_TIMEOUT,this);
            direct_sending_ = false;
        }
        else
        {
            if (evsend_ptr_ != NULL) {
                struct timeval duetv = *tint2tv(duein);
                evtimer_add(evsend_ptr_,&duetv);
                dprintf("%s #%u requeue for %s in %lli\n",tintstr(),id_,tintstr(next_send_time_), duein);
            }
            else
                dprintf("%s #%u cannot requeue for %s, closed\n",tintstr(),id_,tintstr(next_send_time_));
        }
    } else {
        // SAFECLOSE
        dprintf("%s #%u resched, will close\n",tintstr(),id_);
        // Arno: Cannot clean up send events or channel here as we may be
        // LibeventSendCallback() on a specific channel. Do on another event,
        // see ContentTransfer::LibeventCleanCallback()
        this->Schedule4Delete();
    }
}


/*
 * Channel class methods
 */
void Channel::LibeventSendCallback(int fd, short event, void *arg) {

    // Called by libevent when it is the requested send time.
    Time();
    dprintf("%s send callback\n",tintstr() );

    Channel * sender = (Channel*) arg;
    if (NOW<sender->next_send_time_-TINT_MSEC)
        dprintf("%s #%u suspicious send %s<%s\n",tintstr(),
                sender->id(),tintstr(NOW),tintstr(sender->next_send_time_));
    if (sender->next_send_time_ != TINT_NEVER)
        sender->Send();
}

