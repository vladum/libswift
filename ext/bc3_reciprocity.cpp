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
 *  set by the WFQ mechanism. */
class Bc3ReciprocityPolicy : public ReciprocityPolicy {
public:
    virtual void AddPeer (const Address& addr, const Sha1Hash& root) {
        return;
    }

    virtual void DelPeer (const Address& addr, const Sha1Hash& root) {
        return;
    }

    virtual float AdjustNextSendTime(const Address& addr, const Sha1Hash& root, float normal_time) {
        return 1.0;
    }
};
