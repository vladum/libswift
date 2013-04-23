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
        return 1.0;
    }

    void SetScope(const enum Scope scope) {
        scope_ = scope;
    }
private:
    scope_t scope_;

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
};
