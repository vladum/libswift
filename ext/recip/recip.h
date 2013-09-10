/*
 *  recip.h
 *  ReciprocityPolicy implementations.
 *
 *  Created by Vlad Dumitrescu
 *  Copyright 2009-2013 Delft University of Technology. All rights reserved.
 *
 */

#include "swift.h"

using namespace swift;

/* 
 * std::less<Address> functor
 * 
 * This enables the use of swift::Address with std::map (or anything that uses
 * < operator). Uses Address.str() as value.
 */
namespace std {
    template<> struct less<Address> {
       	bool operator() (const Address& lhs, const Address& rhs) {
           	return std::less<const char*>()(lhs.str(), rhs.str());
       	}
    };
}

/* 
 * Base ReciprocityPolicy class.
 *  
 * Parses PEERWEIGHTS message and keeps a map of (endpoint,weight) for each
 * peer. Different algorithms will be implemented by subclasses.
 */
class BaseReciprocityPolicy : public ReciprocityPolicy {
    typedef std::map<Address, int> peerweights_t;

public:
    virtual void AddPeer (const Address& addr);
    virtual void DelPeer (const Address& addr);
    virtual tint SendIntervalFor(Channel *channel);
    virtual bool IsActive();
    virtual void ExternalCmd(char *message);

private:
    peerweights_t peer_weights_;

    void UpdatePeerWeight(const Address& addr, const float weight);
    float GetPeerWeight(const Address &addr);
};

class SelfishReciprocityPolicy : public BaseReciprocityPolicy {
};

class WinnerReciprocityPolicy : public BaseReciprocityPolicy {
public:
	virtual tint SendIntervalFor(Channel *channel);
};

