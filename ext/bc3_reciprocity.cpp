/*
 *  bc3_reciprocity.cpp
 *  swift
 *
 *  Created by Vlad Dumitrescu.
 *  Copyright 2009-2013 Delft University of Technology. All rights reserved.
 *
 */

#include "swift.h"

using namespace swift;

bool recip_extcmd_debug = false;

/** Receives peer weights from the BarterCast3 Dispersy community and
 *  prioritizes peers with higher weights. The normal next send time is being
 *  set by the WFQ mechanism and adjusted here. */
class Bc3ReciprocityPolicy : public ReciprocityPolicy {
public:
    // Prioritization of peers can be handled globally or per-swarm.
    typedef enum Scope {PERSWARM, GLOBAL} scope_t;

    Bc3ReciprocityPolicy() : scope_(GLOBAL) { };

    virtual void AddPeer (const Address& addr, const Sha1Hash& root) {
        // Real implementation is called based on scope setting.
        switch(scope_) {
        case PERSWARM:
            AddPerSwarmPeer(addr, root);
            break;
        case GLOBAL:
            AddGlobalPeer(addr);
            break;
        }
        return;
    }

    virtual void DelPeer (const Address& addr, const Sha1Hash& root) {
        dprintf("%s removing peer %s from bc3 reciprocity\n", tintstr(),
                addr.str());
        return;
    }

    virtual float AdjustNextSendTime(const Address& addr, const Sha1Hash& root,
                                     float normal_time) {
        // TODO(vladum): Implement.
        // TODO(vladum): Peer weights are updated only on addition. This should
        // search in an ordered list of peers. Deletion will just remove a peer
        // from this list.
        int w = GetPeerWeight(addr);
        return 1.0;
    }

    void ExternalCmd(char *message) {
        char *method = NULL, *paramstr = NULL;

        // Split into CMD and PARAMS.
        char *token = strchr(message, ' ');
        if (token != NULL) {
            *token = '\0';
            paramstr = token + 1;
        } else {
            paramstr = "";
        }
        method = message;

        if (recip_extcmd_debug)
            fprintf(stderr,"bc3recip ext cmd: GOT %s %s\n", method, paramstr);

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
                dprintf("%s bc3 reciprocity: bad ext cmd (missing arg)\n",
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
                    dprintf("%s bc3 reciprocity: bad ext cmd (missing arg)\n",
                            tintstr());
                    return;
                }
                ipport[weight_str - ipport - 1] = '\0'; // end of ipport section
                if (!sscanf(weight_str, "%i", &weight)) {
                    dprintf("%s bc3 reciprocity: bad ext cmd (missing arg)\n",
                            tintstr());
                    return;
                }

                // Try to save this pair.
                UpdatePeerWeight(Address(ipport), weight);

                token = strtok_r(NULL, " ", &savetok); // next pair
            }
        }
    }

    void SetScope(const enum Scope scope) {
        scope_ = scope;
    }
private:
    scope_t scope_;
    // TODO(vladum): Use unordered_map?
    typedef std::map<std::string, int>  peerweights_t;
    peerweights_t peer_weights;

    void AddGlobalPeer(const Address& addr) {
        // TODO(vladum): Implement.
        dprintf("%s adding peer %s to global bc3 reciprocity\n",
                tintstr(), addr.str());
        return;
    }

    void AddPerSwarmPeer(const Address& addr, const Sha1Hash& root) {
        // TODO(vladum): Implement.
        return;
    }

    void UpdatePeerWeight(const Address& addr, const int weight) {
        // TODO(vladum): Why is Address.str() the way it is?
        // Should it be thread-safe?
        peer_weights[addr.str()] = weight;

        dprintf("%s bc3 recip peer weights updated to:", tintstr());
        for(peerweights_t::iterator i = peer_weights.begin();
            i != peer_weights.end(); i++) {
            if (i != peer_weights.begin())
                dprintf(",");
            dprintf(" (%s, %d)", i->first.c_str(), i->second);
        }
        dprintf("\n");
    }

    int GetPeerWeight(const Address &addr) {
        peerweights_t::iterator w = peer_weights.find(addr.str());

        if (w != peer_weights.end()) {
            dprintf("%s bc3 recip peer weights: retriving for peer %s => "
                    "weight = %d\n", tintstr(), addr.str(), w->second);
            return w->second;
        } else {
            dprintf("%s bc3 recip peer weights: retriving for peer %s => "
                                "weight = default\n", tintstr(), addr.str());
            return 1; // default weight
        }
    }
};
