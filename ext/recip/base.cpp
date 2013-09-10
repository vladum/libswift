/*
 *  base.cpp
 *  swift
 *
 *  Created by Vlad Dumitrescu.
 *  Copyright 2009-2013 Delft University of Technology. All rights reserved.
 *
 */

#include "recip.h"

using namespace swift;

static bool recip_extcmd_debug = true;

bool BaseReciprocityPolicy::IsActive() {
    return true;
}

void BaseReciprocityPolicy::AddPeer (const Address& addr) {
    fprintf(stderr, "%s adding peer %s to reciprocity\n", tintstr(),
            addr.str());
    peer_weights_[addr] = 0; // neutral reciprocity for now
}

void BaseReciprocityPolicy::DelPeer (const Address& addr) {
    dprintf("%s removing peer %s from reciprocity\n", tintstr(),
            addr.str());
    peer_weights_.erase(addr);
}

tint BaseReciprocityPolicy::SendIntervalFor(Channel *channel) {
    // No requests, nothing to do.
    if (channel->hint_in_size() == 0)
        return 0;

    // The Selfish policy will try to maximize the upload, so reciprocity
    // will only be activated when the upload limit is reached.
    if (channel->transfer().GetCurrentSpeed(DDIR_UPLOAD) <= 
        channel->transfer().GetMaxSpeed(DDIR_UPLOAD))
        return 0;

    int peers = 0;
    tint rate = channel->transfer().GetMaxSpeed(DDIR_UPLOAD) /
                channel->hashtree()->chunk_size();
    uint64_t tot_req = 0;
    std::vector<Channel *>::iterator iter;
    // First channel is always present to handle HANDSHAKEs.
    for (iter = channel->get_channels().begin() + 1;
         iter != channel->get_channels().end(); 
         iter++) {
        Channel *c = *iter;
        // Channels are set to NULL when peers disconnect.
        if (c != NULL) {
            // Only LEDBAT controlled ones. Apperantly, others behave 
            // differently.
            if (c->send_control() == Channel::LEDBAT_CONTROL) {
                tot_req += c->hint_in_size();
                peers++;
                printf("%s #%u tot req from channel %u\t%llu-%llu\n",
                       tintstr(), channel->id(), c->id(), tot_req, 
                       c->hint_in_size());
            }
        }
    }

    // Simple P Controller.
    double feedback_ratio = 
        tot_req ? (double)channel->hint_in_size() / tot_req : 1.0;
    double desired_ratio = 1.0;

    // TODO(vladum): Do actual ranking.
    if (peers == 2) {
        if (channel->id() == 1)
            desired_ratio = 0.8;
        else if (channel->id() == 2)
            desired_ratio = 0.2;
    }
    double error = (desired_ratio - feedback_ratio);
    double p_coef = 0.8;
    double ratio = feedback_ratio + error * p_coef;
    printf("\t\tratio => %lf\n", ratio);
    return TINT_SEC / (rate * ratio);
}

void BaseReciprocityPolicy::ExternalCmd(char *message) {
    char *method = NULL, *paramstr = NULL;

    // Split into CMD and PARAMS.
    char *token = strchr(message, ' ');
    if (token != NULL) {
        *token = '\0';
        paramstr = token + 1;
    } else {
        paramstr = (char *)"";
    }
    method = message;

    if (recip_extcmd_debug)
        fprintf(stderr,"selfishrecip ext cmd: GOT %s %s\n", method, paramstr);

    char *savetok = NULL;
    if (!strcmp(method, "PEERWEIGHTS")) {
        // PEERWEIGHTS ip1:port1:w1 ip2:port2:w2 ...\r\n
        // TODO(vladum): Should we care about the roothash? How many peers
        // should the effort community send? Does the effort community know
        // more about the users? Is there anything else to be known besides
        // ip & port?
        // TODO(vladum): These should be compressed.
        token = strtok_r(paramstr, " ", &savetok); // first (ip:port,weight)
        if (token == NULL) {
            // At least one weight should be specified
            dprintf("%s selfish reciprocity: bad ext cmd (missing arg)\n",
                    tintstr());
            return;
        }

        while (token != NULL) {
            char *ipport, *weight_str;
            int weight;

            // Separate peer identity (ip:port) from weight
            ipport = token;
            weight_str = strrchr(token, ':') + 1;
            if (weight_str == NULL) {
                dprintf("%s selfish reciprocity: bad ext cmd (missing arg)\n",
                        tintstr());
                return;
            }
            ipport[weight_str - ipport - 1] = '\0'; // end of ipport section
            if (!sscanf(weight_str, "%i", &weight)) {
                dprintf("%s selfish reciprocity: bad ext cmd (missing arg)\n",
                        tintstr());
                return;
            }

            // Try to save this pair.
            UpdatePeerWeight(Address(ipport), weight);

            token = strtok_r(NULL, " ", &savetok); // next pair
        }
    }
}

void BaseReciprocityPolicy::UpdatePeerWeight(const Address& addr, 
                                             const float weight) {
    peer_weights_[addr] = weight;
    dprintf("%s updated recip weight for peer %s to %f", tintstr(), 
            addr.str(), weight);
}

float BaseReciprocityPolicy::GetPeerWeight(const Address &addr) {
    peerweights_t::iterator w = peer_weights_.find(addr);

    if (w != peer_weights_.end()) {
        dprintf("%s retrived recip weight for peer %s => "
                "weight = %f\n", tintstr(), w->first.str(), w->second);
        return w->second;
    } else {
        dprintf("%s recip weight for peer %s not found (something bad "
                "happened) => weight = default\n", tintstr(), addr.str());
        return 0.0; // default weight
    }
}
