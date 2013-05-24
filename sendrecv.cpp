/*
 *  sendrecv.cpp
 *  most of the swift's state machine
 *
 *  Created by Victor Grishchenko on 3/6/09.
 *  Copyright 2009-2012 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */
#include "bin_utils.h"
#include "swift.h"
#include <algorithm>  // kill it
#include <cassert>
#include <math.h>
#include <cfloat>
#include "compat.h"

using namespace swift;
using namespace std;

struct event_base *Channel::evbase;
struct event Channel::evrecv;

#define DEBUGTRAFFIC 	0

/** Arno: Victor's design allows a sender to choose some data to push to
 * a receiver, if that receiver is not HINTing at data. Should be disabled
 * when the receiver has a download rate limit.
 */
#define ENABLE_SENDERSIZE_PUSH 0


/** Arno, 2011-11-24: When rate limit is on and the download is in progress
 * we send HINTs for 2 chunks at the moment. This constant can be used to
 * get greater granularity. Set to 0 for original behaviour.
 */
#define HINT_GRANULARITY	2 // chunks

/** Arno, 2012-03-16: Swift can now tunnel data from CMDGW over UDP to
 * CMDGW at another swift instance. This is the default channel ID on UDP
 * for that traffic (cf. overlay swarm).
 */
#define CMDGW_TUNNEL_DEFAULT_CHANNEL_ID		0xffffffff

/*
 TODO  25 Oct 18:55
 - range: ALL
 - randomized testing of advanced ops (new testcase)
 */

void    Channel::AddPeakHashes (struct evbuffer *evb) {
    for(int i=0; i<hashtree()->peak_count(); i++) {
        bin_t peak = hashtree()->peak(i);
        evbuffer_add_8(evb, SWIFT_HASH);
        evbuffer_add_32be(evb, bin_toUInt32(peak));
        evbuffer_add_hash(evb, hashtree()->peak_hash(i));
        char bin_name_buf[32];
        dprintf("%s #%u +phash %s\n",tintstr(),id_,peak.str(bin_name_buf));
    }
}


void    Channel::AddUncleHashes (struct evbuffer *evb, bin_t pos) {

    char bin_name_buf2[32];
    dprintf("%s #%u +uncle hash for %s\n",tintstr(),id_,pos.str(bin_name_buf2));

    bin_t peak = hashtree()->peak_for(pos);
    while (pos!=peak && ((NOW&3)==3 || !pos.parent().contains(data_out_cap_)) &&
            ack_in_.is_empty(pos.parent()) ) {
        bin_t uncle = pos.sibling();
        evbuffer_add_8(evb, SWIFT_HASH);
        evbuffer_add_32be(evb, bin_toUInt32(uncle));
        evbuffer_add_hash(evb,  hashtree()->hash(uncle) );
        char bin_name_buf[32];
        dprintf("%s #%u +hash %s\n",tintstr(),id_,uncle.str(bin_name_buf));
        pos = pos.parent();
    }
}


bin_t           Channel::ImposeHint () {
    uint64_t twist = peer_channel_id_;  // got no hints, send something randomly

    twist &= hashtree()->peak(0).toUInt(); // FIXME may make it semi-seq here

    bin_t my_pick = binmap_t::find_complement(ack_in_, *(hashtree()->ack_out()), twist);

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
    	dprintf("%s #%u dequeued bad time %li\n",tintstr(),id_, last_recv_time_ );
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
            hint_in_.push_back(my_pick);
            hint_in_size_+=my_pick.base_length();
            char bin_name_buf[32];
            dprintf("%s #%u *hint %s\n",tintstr(),id_,my_pick.str(bin_name_buf));
        }
    }
    
    while (!hint_in_.empty() && send.is_none()) {
        bin_t hint = hint_in_.front().bin;
        tint time = hint_in_.front().time;
        hint_in_.pop_front();
        while (!hint.is_base()) { // FIXME optimize; possible attack
            hint_in_.push_front(tintbin(time,hint.right()));
            hint = hint.left();
        }
        //if (time < NOW-TINT_SEC*3/2 )
        //    continue;  bad idea
        if (!ack_in_.is_filled(hint))
            send = hint;
    }

    // Ric: remove from size!
	if (!send.is_none()&&!*retransmitptr)
		hint_in_size_ -= send.base_length();

    uint64_t mass = 0;
    // Arno, 2012-03-09: Is mucho expensive on busy server.
    //for(int i=0; i<hint_in_.size(); i++)
    //    mass += hint_in_[i].bin.base_length();
    char bin_name_buf[32];
    dprintf("%s #%u dequeued %s [%lu]\n",tintstr(),id_,send.str(bin_name_buf),mass);
    return send;
}


void    Channel::AddHandshake (struct evbuffer *evb) {
    if (!peer_channel_id_) { // initiating
        evbuffer_add_8(evb, SWIFT_HASH);
        evbuffer_add_32be(evb, bin_toUInt32(bin_t::ALL));
        evbuffer_add_hash(evb, hashtree()->root_hash());
        dprintf("%s #%u +hash ALL %s\n",
                tintstr(),id_,hashtree()->root_hash().hex().c_str());
    }
    evbuffer_add_8(evb, SWIFT_HANDSHAKE);
    int encoded = -1;
    if (send_control_==CLOSE_CONTROL) {
    	encoded = 0;
    }
    else
    	encoded = EncodeID(id_);
    evbuffer_add_32be(evb, encoded);
    // Ric: add timestamp for offset (time sync.)
    evbuffer_add_64be(evb, NOW);
    dprintf("%s #%u +hs %x\n",tintstr(),id_,encoded);
    have_out_.clear();
}


void    Channel::Send () {

	dprintf("%s #%u Send called \n",tintstr(),id_);

    struct evbuffer *evb = evbuffer_new();
    evbuffer_add_32be(evb, peer_channel_id_);
    bin_t data = bin_t::NONE;
    int evbnonadplen = 0;
    if ( is_established() ) {
    	if (send_control_!=CLOSE_CONTROL) {
			// FIXME: seeder check
			AddHave(evb);
			AddAck(evb);
			if (!hashtree()->is_complete()) {
				AddHint(evb);
				/* Gertjan fix: 7aeea65f3efbb9013f601b22a57ee4a423f1a94d
				"Only call Reschedule for 'reverse PEX' if the channel is in keep-alive mode"
				 */
				//AddPexReq(evb);
				AddCancel(evb);
			}
			//AddPex(evb);
			TimeoutDataOut();
			data = AddData(evb);
    	} else {
    		// Arno: send explicit close
    		AddHandshake(evb);
    	}
    } else {
        AddHandshake(evb);
        AddHave(evb); // Arno, 2011-10-28: from AddHandShake. Why double?
        AddHave(evb);
        AddAck(evb);	// Ric: why sending Ack in Handshake?
    }

    lastsendwaskeepalive_ = (evbuffer_get_length(evb) == 4);

    if (evbuffer_get_length(evb)==4) {// only the channel id; bare keep-alive
        data = bin_t::ALL;
    }
    dprintf("%s #%u sent %ib %s:%x\n",
            tintstr(),id_,(int)evbuffer_get_length(evb),peer().str(),
            peer_channel_id_);
    int r = SendTo(socket_,peer(),evb);
    if (r==-1)
        print_error("swift can't send datagram");
    else
    {
    	raw_bytes_up_ += r;
    	// Arno, 2013-03-08: per-swarm
    	transfer().AddRawBytes(DDIR_UPLOAD,r);
    }
    last_send_time_ = NOW;
    sent_since_recv_++;
    dgrams_sent_++;
    evbuffer_free(evb);
    Reschedule();
}

void    Channel::AddHint (struct evbuffer *evb) {
    std::deque<bin_t> tbc;
	// RATELIMIT
	// Policy is to not send hints when we are above speed limit
	//fprintf(stderr,"AddHint: cur %f max %f\n", transfer().GetCurrentSpeed(DDIR_DOWNLOAD), transfer().GetMaxSpeed(DDIR_DOWNLOAD));

	if (transfer().GetCurrentSpeed(DDIR_DOWNLOAD) > transfer().GetMaxSpeed(DDIR_DOWNLOAD)) {
		if (DEBUGTRAFFIC)
			fprintf(stderr,"hint: forbidden#");
		return;
	}


	// 1. Calc max of what we are allowed to request, uncongested bandwidth wise
    tint plan_for = max(TINT_SEC,rtt_avg_*4);

    tint timed_out = NOW - plan_for*2;
    while ( !hint_out_.empty() && hint_out_.front().time < timed_out ) {
    	bin_t hint = hint_out_.front().bin;
        hint_out_size_ -= hint.base_length();
        hint_out_.pop_front();
        // Ric: send Cancel msg
        tbc.push_back(hint);
    }

    int first_plan_pck = max ( (tint)1, plan_for / dip_avg_ );

    // Riccardo, 2012-04-04: Actually allowed is max minus what we already asked for
    int queue_allowed_hints = max(0,first_plan_pck-(int)hint_out_size_);


    // RATELIMIT
    // 2. Calc max of what is allowed by the rate limiter
    int rate_allowed_hints = LONG_MAX;
    if (transfer().GetMaxSpeed(DDIR_DOWNLOAD) < DBL_MAX)
    {
		uint64_t rough_global_hint_out_size = 0; // rough estimate, as hint_out_ clean up is not done for all channels
		channels_t::iterator iter;
		for (iter=transfer().mychannels_.begin(); iter!=transfer().mychannels_.end(); iter++)
		{
			Channel *c = *iter;
			if (c != NULL)
				rough_global_hint_out_size += c->hint_out_size_;
		}

		// Policy: this channel is allowed to hint at the limit - global_hinted_at
		// Handle MaxSpeed = unlimited
		double rate_hints_limit_float = transfer().GetMaxSpeed(DDIR_DOWNLOAD)/((double)hashtree()->chunk_size());

		int rate_hints_limit = (int)min((double)LONG_MAX,rate_hints_limit_float);

		// Actually allowed is max minus what we already asked for, globally (=all channels)
		rate_allowed_hints = max(0,rate_hints_limit-(int)rough_global_hint_out_size);
    }

    // 3. Take the smallest allowance from rate and queue limit
    uint64_t plan_pck = (uint64_t)min(rate_allowed_hints,queue_allowed_hints);

    // 4. Ask allowance in blocks of chunks to get pipelining going from serving peer.
    if (hint_out_size_ == 0 || plan_pck > HINT_GRANULARITY)
    {
        bin_t hint = transfer().picker().Pick(ack_in_,plan_pck,NOW+plan_for*2);
        if (!hint.is_none()) {
        	if (DEBUGTRAFFIC)
        	{
        		char binstr[32];
        		fprintf(stderr,"hint c%d: ask %s\n", id(), hint.str(binstr) );
        	}
            evbuffer_add_8(evb, SWIFT_HINT);
            evbuffer_add_32be(evb, bin_toUInt32(hint));
            char bin_name_buf[32];
            dprintf("%s #%u +hint %s [%lu]\n",tintstr(),id_,hint.str(bin_name_buf),hint_out_size_);
            dprintf("%s #%u +hint base %s width %llu\n",tintstr(),id_,hint.base_left().str(bin_name_buf), hint.base_length() );
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
            hint_out_.push_back(hint);
            hint_out_size_ += hint.base_length();
            //fprintf(stderr,"send c%d: HINTLEN %i\n", id(), hint.base_length());
            //fprintf(stderr,"HL %i ", hint.base_length());
        }
        else
            dprintf("%s #%u Xhint\n",tintstr(),id_);

        while (!tbc.empty()) {
            cancel_out_.push_back(tbc.front());
            tbc.pop_front();
        }
    }
}


void    Channel::AddCancel (struct evbuffer *evb) {

	char bin_name_buf[32];

	while (SWIFT_MAX_NONDATA_DGRAM_SIZE-evbuffer_get_length(evb) >= 5 && !cancel_out_.empty()) {
		bin_t cancel = cancel_out_.front();
		cancel_out_.pop_front();
		evbuffer_add_8(evb, SWIFT_CANCEL);
		evbuffer_add_32be(evb, bin_toUInt32(cancel));
		dprintf("%s #%u +cancel %s %li\n",
			tintstr(),id_,cancel.str(bin_name_buf),data_in_.time);
	}
}

bin_t        Channel::AddData (struct evbuffer *evb) {

    if (!hashtree()->size()) // know nothing
        return bin_t::NONE;

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

    if (tosend.is_none())// && (last_data_out_time_>NOW-TINT_SEC || data_out_.empty()))
        return bin_t::NONE; // once in a while, empty data is sent just to check rtt FIXED

    if (ack_in_.is_empty() && hashtree()->size())
        AddPeakHashes(evb);

    //NETWVSHASH
    if (hashtree()->get_check_netwvshash())
    	AddUncleHashes(evb,tosend);

    if (!ack_in_.is_empty()) // TODO: cwnd_>1
        data_out_cap_ = tosend;

    // Arno, 2011-11-03: May happen when first data packet is sent to empty
    // leech, then peak + uncle hashes may be so big that they don't fit in eth
    // frame with DATA. Send 2 datagrams then, one with peaks so they have
    // a better chance of arriving. Optimistic violation of atomic datagram
    // principle.
    if (hashtree()->chunk_size() == SWIFT_DEFAULT_CHUNK_SIZE && evbuffer_get_length(evb) > SWIFT_MAX_NONDATA_DGRAM_SIZE) {
        dprintf("%s #%u fsent %ib %s:%x\n",
                tintstr(),id_,(int)evbuffer_get_length(evb),peer().str(),
                peer_channel_id_);
    	int ret = Channel::SendTo(socket_,peer(),evb); // kind of fragmentation
    	if (ret > 0)
    	{
	    raw_bytes_up_ += ret;
	    // Arno, 2013-03-08: per-swarm
	    transfer().AddRawBytes(DDIR_UPLOAD,ret);
    	}
        evbuffer_add_32be(evb, peer_channel_id_);
    }

    if (hashtree()->chunk_size() != SWIFT_DEFAULT_CHUNK_SIZE && isretransmit) {
    	/* FRAGRAND
    	 * Arno, 2012-01-17: We observe strange behaviour when using
    	 * fragmented UDP packets. When ULANC sends a specific datagram ("995"),
    	 * the 2nd IP packet carrying it gets lost structurally. When
    	 * downloading from the same asset hosted on a Linux 32-bit machine
    	 * using a Win7 32-bit client (behind a NAT), one specific full
    	 * datagram never gets delivered (6970 one before do). A workaround
    	 * is to add some random data to the datagram. Hence we introduce
    	 * the SWIFT_RANDOMIZE message, that is added to the datagram carrying
    	 * the DATA on a retransmit.
    	 */
 	     char binstr[32];
         fprintf(stderr,"AddData: retransmit of randomized chunk %s\n",tosend.str(binstr) );
         evbuffer_add_8(evb, SWIFT_RANDOMIZE);
         evbuffer_add_32be(evb, (int)rand() );
    }

    evbuffer_add_8(evb, SWIFT_DATA);

    // Ric: add timestamp as for the LEDBAT draft specs
    evbuffer_add_64be(evb, NOW);

    evbuffer_add_32be(evb, bin_toUInt32(tosend));

    struct evbuffer_iovec vec;
    if (evbuffer_reserve_space(evb, hashtree()->chunk_size(), &vec, 1) < 0) {
		print_error("error on evbuffer_reserve_space");
		return bin_t::NONE;
    }
    size_t r = transfer().GetStorage()->Read((char *)vec.iov_base,
		     hashtree()->chunk_size(),tosend.base_offset()*hashtree()->chunk_size());
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
    // Arno, 2013-03-08: per-swarm
    transfer().AddBytes(DDIR_UPLOAD,r);

    char bin_name_buf[32];
    dprintf("%s #%u +data %s\n",tintstr(),id_,tosend.str(bin_name_buf));

    // RATELIMIT
    // ARNOSMPTODO: count overhead bytes too? Move to Send() then.
	transfer_->OnSendData(hashtree()->chunk_size());

    return tosend;
}


void    Channel::AddAck (struct evbuffer *evb) {
    if (data_in_==tintbin())
	//if (data_in_.bin==bin64_t::NONE)
        return;
    // sometimes, we send a HAVE (e.g. in case the peer did repetitive send)
    evbuffer_add_8(evb, data_in_.time==TINT_NEVER?SWIFT_HAVE:SWIFT_ACK);
    evbuffer_add_32be(evb, bin_toUInt32(data_in_.bin));
    // Ric: data_in_.time represents the calculated owd
    if (data_in_.time!=TINT_NEVER)
        evbuffer_add_64be(evb, data_in_.time);


	if (DEBUGTRAFFIC)
		fprintf(stderr,"send c%d: ACK %i\n", id(), bin_toUInt32(data_in_.bin));

    have_out_.set(data_in_.bin);
    char bin_name_buf[32];
    dprintf("%s #%u +ack %s owd: %li\n",
        tintstr(),id_,data_in_.bin.str(bin_name_buf),data_in_.time);
    if (data_in_.bin.layer()>2)
        data_in_dbl_ = data_in_.bin;

    //fprintf(stderr,"data_in_ c%d\n", id() );
    data_in_ = tintbin();
    //data_in_ = tintbin(NOW,bin64_t::NONE);
}


void    Channel::AddHave (struct evbuffer *evb) {
    if (!data_in_dbl_.is_none()) { // TODO: do redundancy better
        evbuffer_add_8(evb, SWIFT_HAVE);
        evbuffer_add_32be(evb, bin_toUInt32(data_in_dbl_));
        data_in_dbl_=bin_t::NONE;
    }
    if (DEBUGTRAFFIC)
		fprintf(stderr,"send c%d: HAVE ",id() );

	// ZEROSTATE
	if (transfer().IsZeroState())
	{
		if (is_established())
			return;

		// Say we have peaks
        for(int i=0; i<hashtree()->peak_count(); i++) {
            bin_t peak = hashtree()->peak(i);
			evbuffer_add_8(evb, SWIFT_HAVE);
            evbuffer_add_32be(evb, bin_toUInt32(peak));
			char bin_name_buf[32];
		    dprintf("%s #%u +have %s\n",tintstr(),id_,peak.str(bin_name_buf));
        }
		return;
	}

    for(int count=0; count<4; count++) {
        bin_t ack = binmap_t::find_complement(have_out_, *(hashtree()->ack_out()), 0); // FIXME: do rotating queue
        if (ack.is_none())
            break;
        ack = hashtree()->ack_out()->cover(ack);
        have_out_.set(ack);
        evbuffer_add_8(evb, SWIFT_HAVE);
        evbuffer_add_32be(evb, bin_toUInt32(ack));

    	if (DEBUGTRAFFIC)
    		fprintf(stderr," %i", bin_toUInt32(ack));

        char bin_name_buf[32];
        dprintf("%s #%u +have %s\n",tintstr(),id_,ack.str(bin_name_buf));
    }
	if (DEBUGTRAFFIC)
		fprintf(stderr,"\n");

}


void    Channel::Recv (struct evbuffer *evb) {
    dprintf("%s #%u recvd %ib\n",tintstr(),id_,(int)evbuffer_get_length(evb)+4);
    dgrams_rcvd_++;

    if (!transfer().IsOperational()) {
    	dprintf("%s #%u recvd on broken transfer %d \n",tintstr(),id_, transfer().fd() );
		CloseOnError();
    	return;
    }

    lastrecvwaskeepalive_ = (evbuffer_get_length(evb) == 0);
    if (lastrecvwaskeepalive_)
    	// Update speed measurements such that they decrease when DL stops
    	transfer().OnRecvData(0);

    if (last_send_time_ && rtt_avg_==TINT_SEC && dev_avg_==0) {
        rtt_avg_ = NOW - last_send_time_;
        dev_avg_ = rtt_avg_;
        dip_avg_ = rtt_avg_;
        // Ric: update the RTT early when communicating with a seeder
        last_rtt_update_ = last_send_time_ + rtt_avg_;
        //if (time_offset_==0) {
        //	time_offset_ =  tintabs(time_offset_) - rtt_avg_>>1;
        //	dprintf("%s #%u time offset %lli\n",tintstr(),id_,time_offset_);
        //}
        dprintf("%s #%u sendctrl rtt init %li\n",tintstr(),id_,rtt_avg_);
    }

    bin_t data = evbuffer_get_length(evb) ? bin_t::NONE : bin_t::ALL;

	if (DEBUGTRAFFIC)
		fprintf(stderr,"recv c%d: size %lu ", id(), evbuffer_get_length(evb));

	while (evbuffer_get_length(evb)) {
        uint8_t type = evbuffer_remove_8(evb);

        if (DEBUGTRAFFIC)
        	fprintf(stderr," %d", type);

        switch (type) {
            case SWIFT_HANDSHAKE:
            	OnHandshake(evb);
            	break;
            case SWIFT_DATA:
            	if (!transfer().IsZeroState())
            		data=OnData(evb);
            	else
            		OnDataZeroState(evb);
            	break;
            case SWIFT_HAVE:
            	if (!transfer().IsZeroState())
            		OnHave(evb);
            	else
            		OnHaveZeroState(evb);
            	break;
            case SWIFT_ACK:
            	OnAck(evb);
            	break;
            case SWIFT_HASH:
            	if (!transfer().IsZeroState())
            		OnHash(evb);
            	else
            		OnHashZeroState(evb);
            	break;
            case SWIFT_HINT:
            	OnHint(evb);
            	break;
            case SWIFT_CANCEL:
            	OnCancel(evb);
            	break;
            case SWIFT_PEX_ADD:
            	if (!transfer().IsZeroState())
            		OnPexAdd(evb);
            	else
            		OnPexAddZeroState(evb);
            	break;
            case SWIFT_PEX_REQ:
            	if (!transfer().IsZeroState())
            		OnPexReq();
            	else
            		OnPexReqZeroState(evb);
            	break;
            case SWIFT_RANDOMIZE:
            	OnRandomize(evb);
            	break; //FRAGRAND
            default:
                dprintf("%s #%u ?msg id unknown %i\n",tintstr(),id_,(int)type);
                return;
        }
    }
	if (DEBUGTRAFFIC)
    {
    	fprintf(stderr,"\n");
    }

    last_recv_time_ = NOW;
    sent_since_recv_ = 0;


    // Arno: see if transfer still in working order
    transfer().UpdateOperational();
    if (!transfer().IsOperational()) {
    	dprintf("%s #%u recvd broke transfer %d \n",tintstr(),id_, transfer().fd() );
        CloseOnError();
        return;
    }

    Reschedule();
}


void   Channel::CloseOnError()
{
	Close();
	// set established->false after Close, so Close does send explicit close.
	// RecvDatagram will schedule this for delete.
	peer_channel_id_ = 0;
	return;
}


/*
 * Arno: FAXME: HASH+DATA should be handled as a transaction: only when the
 * hashes check out should they be stored in the hashtree, otherwise revert.
 */
void    Channel::OnHash (struct evbuffer *evb) {
	bin_t pos = bin_fromUInt32(evbuffer_remove_32be(evb));
    Sha1Hash hash = evbuffer_remove_hash(evb);
    hashtree()->OfferHash(pos,hash);
    char bin_name_buf[32];
    dprintf("%s #%u -hash %s\n",tintstr(),id_,pos.str(bin_name_buf));

    //fprintf(stderr,"HASH %lli hex %s\n",pos.toUInt(), hash.hex().c_str() );
}


void    Channel::CleanHintOut (bin_t pos) {
    int hi = 0;
    while (hi<hint_out_.size() && !hint_out_[hi].bin.contains(pos))
        hi++;
    if (hi==hint_out_.size())
        return; // something not hinted or hinted in far past
    // TODO Ric: remove only the received data from the list
    // TODO maybe send a CANCEL msg?
    while (hi--) { // removing likely snubbed hints
    	bin_t hint = hint_out_.front().bin;
        hint_out_size_ -= hint.base_length();
        hint_out_.pop_front();
        // Ric: send Cancel msgs
        cancel_out_.push_back(hint);
    }

    /* Ric: move it to the front
    tintbin tmp = hint_out_[hi];
    hint_out_.erase(hint_out_.begin() + hi);
    hint_out_.push_front(tmp);
    */

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
    hint_out_.pop_front();
    hint_out_size_--;
}


bin_t Channel::OnData (struct evbuffer *evb) {  // TODO: HAVE NONE for corrupted data

	char bin_name_buf[32];
    tint sender_time = evbuffer_remove_64be(evb); // FIXME 32
	bin_t pos = bin_fromUInt32(evbuffer_remove_32be(evb));

    // Arno: Assuming DATA last message in datagram
    if (evbuffer_get_length(evb) > hashtree()->chunk_size()) {
    	dprintf("%s #%u !data chunk size mismatch %s: exp %u got " PRISIZET "\n",tintstr(),id_,pos.str(bin_name_buf), hashtree()->chunk_size(), evbuffer_get_length(evb));
    	fprintf(stderr,"WARNING: chunk size mismatch: exp %u got " PRISIZET "\n",hashtree()->chunk_size(), evbuffer_get_length(evb));
    }

    int length = (evbuffer_get_length(evb) < hashtree()->chunk_size()) ? evbuffer_get_length(evb) : hashtree()->chunk_size();
    if (!hashtree()->ack_out()->is_empty(pos)) {
        // Arno, 2012-01-24: print message for duplicate
        dprintf("%s #%u Ddata %s\n",tintstr(),id_,pos.str(bin_name_buf));
        evbuffer_drain(evb, length);
        data_in_ = tintbin(TINT_NEVER,transfer().ack_out()->cover(pos));

        // Arno, 2012-01-24: Make sure data interarrival periods don't get
        // screwed up because of these (ignored) duplicates.
        UpdateDIP(pos);
        return bin_t::NONE;
    }
    uint8_t *data = evbuffer_pullup(evb, length);

    // Ric: set data_in time to the calculated owd
    tint owd = NOW-(sender_time-time_offset_);
    if (owds_.size()>=10)		// TODO why 10?
		owds_.pop_front();
	owds_.push_back(owd);
    // Ric: If we are communicating with a seeder, update the rtt value if older
    //      than 1 sec. WE don't updated it for every data pkt as we do not know
	//      when a request is actually processed by the sender.
    //      TODO: make the update time dependent on the DL speed!
    if (last_rtt_update_+(TINT_SEC<<2)<NOW || owds_.size() < 10) {
    	UpdateRTT();
    	dprintf("%s #%u sendctrl rtt %li dev %li dip %li based on the last %lu owd samples\n",
    	                tintstr(),id_,rtt_avg_,dev_avg_, dip_avg_,owds_.size());
    }

	data_in_ = tintbin(owd,bin_t::NONE);

    if (!hashtree()->OfferData(pos, (char*)data, length)) {
    	evbuffer_drain(evb, length);
        char bin_name_buf[32];
        dprintf("%s #%u !data %s\n",tintstr(),id_,pos.str(bin_name_buf));
        return bin_t::NONE;
    }
    evbuffer_drain(evb, length);
    dprintf("%s #%u -data %s\n",tintstr(),id_,pos.str(bin_name_buf));

    if (DEBUGTRAFFIC)
    	fprintf(stderr,"$ ");

    bin_t cover = transfer().ack_out()->cover(pos);
    for(int i=0; i<transfer().cb_installed; i++)
        if (cover.layer()>=transfer().cb_agg[i])
            transfer().callbacks[i](transfer().fd(),cover);  // FIXME
    if (cover.layer() >= 5) // Arno: tested with 32K, presently = 2 ** 5 * chunk_size CHUNKSIZE
    	transfer().OnRecvData( pow((double)2,(double)5)*((double)hashtree()->chunk_size()) );
    data_in_.bin = pos;

    UpdateDIP(pos);
    CleanHintOut(pos);

    bytes_down_ += length;
    global_bytes_down += length;
    // Arno, 2013-03-08: per-swarm
    transfer().AddBytes(DDIR_DOWNLOAD,length); // should be same as hashtree().complete()

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


void Channel::UpdateRTT(tint rtt)
{
	// Ric: if we are communicating with a seeder, update the rtt based on
	//      the last owd calculations.
	if (!rtt) {
		tint tmp = 0;
		for (uint i=0; i < owds_.size(); i++)
			tmp += owds_[i];
		// Ric: assume rtt to be twice the owd and the processing time at the
		//      sender comparable to the dip;
		rtt = tmp*2/owds_.size() + dip_avg_;
	}
    rtt_avg_ = (rtt_avg_*7 + rtt) >> 3;
    dev_avg_ = ( dev_avg_*3 + tintabs(rtt-rtt_avg_) ) >> 2;
    last_rtt_update_ = NOW;
}


void    Channel::OnAck (struct evbuffer *evb) {
    bin_t ackd_pos = bin_fromUInt32(evbuffer_remove_32be(evb));
    tint peer_owd_time = evbuffer_remove_64be(evb); // FIXME 32
    // FIXME FIXME: wrap around here
    if (ackd_pos.is_none())
        return; // likely, broken chunk/ insufficient hashes
    if (hashtree()->size() && ackd_pos.base_offset()>=hashtree()->size_in_chunks()) {
        char bin_name_buf[32];
        eprintf("invalid ack: %s\n",ackd_pos.str(bin_name_buf));
        return;
    }
    ack_in_.set(ackd_pos);

    //fprintf(stderr,"OnAck: got bin %s is_complete %d\n", ackd_pos.str(), (int)ack_in_.is_complete_arno( hashtree()->ack_out()->get_height() ));

    int di = 0, ri = 0;
    // find an entry for the send (data out) event
    while (  di<data_out_.size() && ( data_out_[di]==tintbin() ||
           !ackd_pos.contains(data_out_[di].bin) )  )
        di++;
    // FUTURE: delayed acks
    // rule out retransmits
    while (  ri<data_out_tmo_.size() && !ackd_pos.contains(data_out_tmo_[ri].bin) )
        ri++;
    char bin_name_buf[32];
    dprintf("%s #%u %cack %s\n",tintstr(),id_,
            di==data_out_.size()?'?':'-',ackd_pos.str(bin_name_buf));
    if (di!=data_out_.size() && ri==data_out_tmo_.size()) { // not a retransmit
        // round trip time calculations
        UpdateRTT(NOW-data_out_[di].time);
        assert(data_out_[di].time!=TINT_NEVER);
        // one-way delay calculations
        owd_cur_bin_ = 0;//(owd_cur_bin_+1) & 3;
        owd_current_[owd_cur_bin_] = peer_owd_time;
        if ( owd_min_bin_start_+TINT_SEC*30 < NOW ) {
            owd_min_bin_start_ = NOW;
            owd_min_bin_ = (owd_min_bin_+1) & 3;
            owd_min_bins_[owd_min_bin_] = TINT_NEVER;
        }
        if (owd_min_bins_[owd_min_bin_]>peer_owd_time)
            owd_min_bins_[owd_min_bin_] = peer_owd_time;
        dprintf("%s #%u sendctrl rtt %li dev %li based on %s\n",
                tintstr(),id_,rtt_avg_,dev_avg_,data_out_[di].bin.str(bin_name_buf));
        ack_rcvd_recent_++;
        // early loss detection by packet reordering
        for (int re=0; re<di-MAX_REORDERING; re++) {
            if (data_out_[re]==tintbin())
                continue;
            ack_not_rcvd_recent_++;
            data_out_tmo_.push_back(data_out_[re].bin);
            dprintf("%s #%u Rdata %s\n",tintstr(),id_,data_out_.front().bin.str(bin_name_buf));
            data_out_cap_ = bin_t::ALL;
            data_out_[re] = tintbin();
        }
    }
    if (di!=data_out_.size())
        data_out_[di]=tintbin();
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
            char bin_name_buf[32];
            dprintf("%s #%u Tdata %s\n",tintstr(),id_,data_out_.front().bin.str(bin_name_buf));
        }
        data_out_.pop_front();
    }
    // clear retransmit queue of older items
    while (!data_out_tmo_.empty() && data_out_tmo_.front().time<NOW-MAX_POSSIBLE_RTT)
        data_out_tmo_.pop_front();
}


void Channel::OnHave (struct evbuffer *evb) {
    bin_t ackd_pos = bin_fromUInt32(evbuffer_remove_32be(evb));
    if (ackd_pos.is_none())
        return; // wow, peer has hashes

    // PPPLUG
    if (ENABLE_VOD_PIECEPICKER) {
		// Ric: check if we should set the size in the file transfer
		if (transfer().availability().size() <= 0 && hashtree()->size() > 0)
		{
			transfer().availability().setSize(hashtree()->size_in_chunks());
		}
		// Ric: update the availability if needed
		transfer().availability().set(id_, ack_in_, ackd_pos);
    }

    ack_in_.set(ackd_pos);
    char bin_name_buf[32];
    dprintf("%s #%u -have %s\n",tintstr(),id_,ackd_pos.str(bin_name_buf));

    //fprintf(stderr,"OnHave: got bin %s is_complete %d\n", ackd_pos.str(), IsComplete() );

}


void    Channel::OnHint (struct evbuffer *evb) {
    bin_t hint = bin_fromUInt32(evbuffer_remove_32be(evb));
    // FIXME: wake up here
    hint_in_.push_back(hint);
    // Ric: update the hint_size
    hint_in_size_ += hint.base_length();
    char bin_name_buf[32];
    dprintf("%s #%u -hint %s [%lu]\n",tintstr(),id_,hint.str(bin_name_buf), hint_in_size_);
}


void Channel::OnHandshake (struct evbuffer *evb) {

	uint32_t pcid = evbuffer_remove_32be(evb);
	uint64_t remote_time = evbuffer_remove_64be(evb);
    dprintf("%s #%u -hs %x\n",tintstr(),id_,pcid);

    //if (DEBUGTRAFFIC)
    //	fprintf(stderr, "recv timestamp %s\toffset%lu%", tintstr(remote_time), time_offset_);

    if (is_established() && pcid == 0) {
    	// Arno: received explicit close
    	peer_channel_id_ = 0; // == established -> false
    	Close();
    	return;
    }

    peer_channel_id_ = pcid;
    // self-connection check

    if (!SELF_CONN_OK) {
        uint32_t try_id = DecodeID(peer_channel_id_);
        // Arno, 2012-05-29: Fixed duplicate test
        if (channel(try_id) && channel(try_id)->peer_channel_id_) {
            peer_channel_id_ = 0;
            Close();
            return; // this is a self-connection
        }
    }

    // FUTURE: channel forking
    if (is_established()) {
        dprintf("%s #%u established %s\n", tintstr(), id_, peer().str());
		// Ric: init the time offset
		if (rtt_avg_!=TINT_SEC && time_offset_==0) {
			time_offset_ = remote_time-NOW;
			//time_offset_ = time_offset_>0 ? time_offset_ - (rtt_avg_>>1) : time_offset_ + (rtt_avg_>>1);
			time_offset_ += rtt_avg_>>1;
			dprintf("%s #%u time offset %li\n",tintstr(),id_,time_offset_);
		}
    }
}

void    Channel::OnCancel (struct evbuffer *evb) {

	char bin_name_buf[32];
    bin_t cancel = bin_fromUInt32(evbuffer_remove_32be(evb));

    int hi = 0;
    while (hi<hint_in_.size() && !cancel.contains(hint_in_[hi].bin))
        hi++;

    // nothing to cancel
    if (hi==hint_in_.size())
        return;

    dprintf("%s #%u -cancel %s => removing: ",tintstr(),id_,cancel.str(bin_name_buf));
    do {
    	dprintf("%s ",hint_in_[hi].bin.str(bin_name_buf));
    	hint_in_size_ -= hint_in_[hi].bin.base_length();
    	hint_in_.erase(hint_in_.begin()+hi);
    	hi++;
    } while (cancel.contains(hint_in_[hi].bin));
    dprintf("\n");

}

void Channel::OnPexAdd (struct evbuffer *evb) {
    uint32_t ipv4 = evbuffer_remove_32be(evb);
    uint16_t port = evbuffer_remove_16be(evb);
    Address addr(ipv4,port);
    dprintf("%s #%u -pex %s\n",tintstr(),id_,addr.str());
    if (transfer().OnPexAddIn(addr))
        useless_pex_count_ = 0;
    else
    {
		dprintf("%s #%u already channel to %s\n", tintstr(),id_,addr.str());
        useless_pex_count_++;
    }
    pex_request_outstanding_ = false;
}


//FRAGRAND
void Channel::OnRandomize (struct evbuffer *evb) {
    dprintf("%s #%u -rand\n",tintstr(),id_ );
	// Payload is 4 random bytes
    uint32_t r = evbuffer_remove_32be(evb);
}


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
            	evbuffer_add_8(evb, SWIFT_PEX_ADD);
            	evbuffer_add_32be(evb, a.ipv4());
            	evbuffer_add_16be(evb, a.port());
            	dprintf("%s #%u +pex (reverse) %s\n",tintstr(),id_,a.str());
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
    int chid = 0, tries=0;
    Address a;
    while (true)
    {
    	// Arno, 2011-10-03: Choosing Gertjan's RandomChannel over RevealChannel here.
    	chid = transfer().RandomChannel(id_);
    	if (chid==-1 || chid==id_ || tries > 5) {
    		pex_requested_ = false;
    		return;
    	}
    	a = channels[chid]->peer();
    	if (!a.is_private() || (a.is_private() && peer().is_private()))
    		break;
    	tries++;
    }

    evbuffer_add_8(evb, SWIFT_PEX_ADD);
    evbuffer_add_32be(evb, a.ipv4());
    evbuffer_add_16be(evb, a.port());
    dprintf("%s #%u +pex %s\n",tintstr(),id_,a.str());

    pex_requested_ = false;
    /* Ensure that we don't add the same id to the reverse_pex_out_ queue
       more than once. */
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
    if (transfer().hs_in_.size() >= SWIFT_MAX_CONNECTIONS ||
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
    RecvDatagram(fd);
    event_add(&evrecv, NULL);
}

void    Channel::RecvDatagram (evutil_socket_t socket) {
    struct evbuffer *evb = evbuffer_new();
    Address addr;

    RecvFrom(socket, addr, evb);
    size_t evboriglen = evbuffer_get_length(evb);

//#define return_log(...) { fprintf(stderr,__VA_ARGS__); evbuffer_free(evb); return; }
#define return_log(...) { dprintf(__VA_ARGS__); evbuffer_free(evb); return; }
    if (evbuffer_get_length(evb)<4)
        return_log("socket layer weird: datagram < 4 bytes from %s (prob ICMP unreach)\n",addr.str());
    uint32_t mych = evbuffer_remove_32be(evb);
    Sha1Hash hash;
    Channel* channel = NULL;
    if (mych==0) { // peer initiates handshake
        if (evbuffer_get_length(evb)<1+4+1+4+Sha1Hash::SIZE)
            return_log ("%s #0 incorrect size %i initial handshake packet %s\n",
                        tintstr(),(int)evbuffer_get_length(evb),addr.str());
        uint8_t hashid = evbuffer_remove_8(evb);
        if (hashid!=SWIFT_HASH)
            return_log ("%s #0 no hash in the initial handshake %s\n",
                        tintstr(),addr.str());
        bin_t pos = bin_fromUInt32(evbuffer_remove_32be(evb));
        if (!pos.is_all())
            return_log ("%s #0 that is not the root hash %s\n",tintstr(),addr.str());
        hash = evbuffer_remove_hash(evb);
        FileTransfer* ft = FileTransfer::Find(hash);
        if (!ft)
        {
            ZeroState *zs = ZeroState::GetInstance();
            ft = zs->Find(hash);
            if (!ft)
                return_log ("%s #0 hash %s unknown, requested by %s\n",tintstr(),hash.hex().c_str(),addr.str());
        }
	else if (ft->IsZeroState() && !ft->hashtree()->is_complete())
	{
	    return_log ("%s #0 zero hash %s broken, requested by %s\n",tintstr(),hash.hex().c_str(),addr.str());
	}
        if (!ft->IsOperational())
        {
            return_log ("%s #0 hash %s broken, requested by %s\n",tintstr(),hash.hex().c_str(),addr.str());
        }

        dprintf("%s #0 -hash ALL %s\n",tintstr(),hash.hex().c_str());

        // Arno, 2012-02-27: Check for duplicate channel
        Channel* existchannel = ft->FindChannel(addr,NULL);
        if (existchannel)
        {
             // Arno: 2011-10-13: Ignore if established, otherwise consider
             // it a concurrent connection attempt.
             if (existchannel->is_established()) {
                  // ARNOTODO: Read complete handshake here so we know whether
                  // attempt is to new channel or to existing. Currently read
                  // in OnHandshake()
                  //
		  return_log("%s #0 have a channel already to %s\n",tintstr(),addr.str());
	     } else {
		channel = existchannel;
		//fprintf(stderr,"Channel::RecvDatagram: HANDSHAKE: reuse channel %s\n", channel->peer_.str() );
	     }
        }
        if (channel == NULL) {
        	//fprintf(stderr,"Channel::RecvDatagram: HANDSHAKE: create new channel %s\n", addr.str() );
        	channel = new Channel(ft, socket, addr);
        }
        //fprintf(stderr,"CHANNEL INCOMING DEF hass %s is id %d\n",hash.hex().c_str(),channel->id());

    } else if (mych==CMDGW_TUNNEL_DEFAULT_CHANNEL_ID) {
    	// SOCKTUNNEL
    	CmdGwTunnelUDPDataCameIn(addr,CMDGW_TUNNEL_DEFAULT_CHANNEL_ID,evb);
    	evbuffer_free(evb);
    	return;
    } else { // peer responds to my handshake (and other messages)
        mych = DecodeID(mych);
        if (mych>=channels.size())
            return_log("%s invalid channel #%u, %s\n",tintstr(),mych,addr.str());
        channel = channels[mych];
        if (!channel)
            return_log ("%s #%u is already closed\n",tintstr(),mych);
	if (channel->IsDiffSenderOrDuplicate(addr,mych)) {
	    channel->Schedule4Close();
	    return_log ("%s #%u is duplicate\n",tintstr(),mych);
	}
        channel->own_id_mentioned_ = true;
    }
    channel->raw_bytes_down_ += evboriglen;
    // Arno, 2013-03-08: per-swarm
    channel->transfer().AddRawBytes(DDIR_DOWNLOAD,evboriglen);


    //dprintf("recvd %i bytes for %i\n",data.size(),channel->id);
    bool wasestablished = channel->is_established();

    //dprintf("%s #%u peer %s recv_peer %s addr %s\n", tintstr(),mych, channel->peer().str(), channel->recv_peer().str(), addr.str() );

    channel->Recv(evb);

    evbuffer_free(evb);
    //SAFECLOSE
    if (wasestablished && !channel->is_established()) {
    	// Arno, 2012-01-26: Received an explict close, clean up channel, safely.
    	channel->Schedule4Close();
    }
}



/*
 * Channel instance methods
 */

void Channel::CloseChannelByAddress(const Address &addr)
{
	// fprintf(stderr,"CloseChannelByAddress: address is %s\n", addr.str() );
	channels_t::iterator iter;
    for (iter = channels.begin(); iter != channels.end(); iter++)
    {
		Channel *c = *iter;
		if (c != NULL && c->peer_ == addr)
		{
			// ARNOSMPTODO: will do another send attempt before not being
			// Rescheduled.
			c->peer_channel_id_ = 0; // established->false, do no more sending
			c->Schedule4Close();
			break;
		}
    }
}


void Channel::Close () {

	this->SwitchSendControl(CLOSE_CONTROL);

    if (is_established())
    	this->Send(); // Arno: send explicit close

	if (!transfer().IsZeroState() && ENABLE_VOD_PIECEPICKER) {
		// Ric: remove its binmap from the availability
		transfer().availability().remove(id_, ack_in_);
    }

    // SAFECLOSE
    // Arno: ensure LibeventSendCallback is no longer called with ptr to this Channel
    ClearEvents();
}


static int xxx = -1;

void Channel::Reschedule () {
    xxx++;
	tint pktsRate = 0;

	// RATELIMIT
	if (transfer().GetCurrentSpeed(DDIR_UPLOAD) > transfer().GetMaxSpeed(DDIR_UPLOAD) && hint_in_size_ > 1) {

		int peers = 0;
		tint rate = transfer().GetMaxSpeed(DDIR_UPLOAD) / hashtree()->chunk_size();
		uint64_t tot_req = 0;
		std::vector<Channel *>::iterator iter;
		for (iter = channels.begin()+1; iter != channels.end(); iter++)
		{
			Channel *c = *iter;
			if (c != NULL)
			{
				// Ric: check why? :-|
				if (c->send_control_==LEDBAT_CONTROL) {
					tot_req += c->hint_in_size_;
					peers++;
					//if (xxx % 100 == 0) fprintf(stderr, "\e[41m%s #%u tot req from channel %i\t%lu-%lu\e[0m\n",tintstr(),id_, c->id_,tot_req,c->hint_in_size_);
				}
			}
		}

		float ratio = tot_req ? float(hint_in_size_)/float(tot_req) : 1.0;
		//if (xxx % 100 == 0) fprintf(stderr, "\t\t\t\t ratio: %f", ratio);
		// deviate the ratio to an equal share

		if (ratio!=1.0) {
			// Ric: TODO work after fixing ledbat!
			ratio += (1.0 / (peers) - ratio) * 1.0/(peers);
			//ratio = 1.0/peers;
		}
		//if (xxx % 100 == 0) fprintf(stderr, "=>%f\n",ratio);

		pktsRate = TINT_SEC/(rate*ratio);
		fprintf(stderr, "pktsRate: %li\n",pktsRate);
		//transfer().OnSendNoData();

	}

	// Arno: CAREFUL: direct send depends on diff between next_send_time_ and
	// NOW to be 0, so any calls to Time in between may put things off. Sigh.
	Time();
    next_send_time_ = NextSendTime();

    if (next_send_time_!=TINT_NEVER) {
    	assert(next_send_time_<NOW+TINT_MIN);
        tint duein = next_send_time_-NOW;

    	if (pktsRate) {
    		duein = max(duein, last_send_time_+pktsRate-NOW);
    		//fprintf(stderr, " %li %li %li\n" ,last_send_time_, pktsRate, NOW);
    		fprintf(stderr, " duein: %li\n", duein);
    		//fprintf(stderr, "%lu\n", hint_in_size_);
    		//fprintf(stderr, "%s #%u Upload Limitation\tspeed:%02lf limit:%lf   duein:%li hint_in:%lu\n",tintstr(),id_,transfer().GetCurrentSpeed(DDIR_UPLOAD), transfer().GetMaxSpeed(DDIR_UPLOAD), duein,hint_in_size_);
    	}


        if (duein <= 0 && !direct_sending_ && next_send_time_<NOW) {
        	// Arno, 2011-10-18: libevent's timer implementation appears to be
        	// really slow, i.e., timers set for 100 usec from now get called
        	// at least two times later :-( Hence, for sends after receives
        	// perform them directly.
        	dprintf("%s #%u requeue direct send\n",tintstr(),id_);
            direct_sending_ = true;
        	LibeventSendCallback(-1,EV_TIMEOUT,this);
            direct_sending_ = false;
        }
        else {
        	if (evsend_ptr_ != NULL) {
        		struct timeval duetv = *tint2tv(duein);
        		evtimer_add(evsend_ptr_,&duetv);
        		dprintf("%s #%u requeue for %s in %llu\n",tintstr(),id_,tintstr(next_send_time_+duein), (uint64_t)duein);
        	}
        	else
        	    dprintf("%s #%u cannot requeue for %s, closed\n",tintstr(),id_,tintstr(next_send_time_));
        }
    } else {
    	// SAFECLOSE
        fprintf(stderr, "%s #%u resched, will close\n",tintstr(),id_);
		this->Schedule4Close();
    }
}


/*
 * Channel class methods
 */
void Channel::LibeventSendCallback(int fd, short event, void *arg) {

	// Called by libevent when it is the requested send time.
    Time();
    Channel * sender = (Channel*) arg;
    if (NOW<sender->next_send_time_-TINT_MSEC)
        dprintf("%s #%u suspicious send %s<%s\n",tintstr(),
                sender->id(),tintstr(NOW),tintstr(sender->next_send_time_));
    if (sender->next_send_time_ != TINT_NEVER)
    	sender->Send();
}

