/*
 *  zerohashtree.cpp
 *  a hashtree interface implemented by reading hashes from a prepared .mhash
 *  file on disk directly, to save memory.
 *
 *  Created by Victor Grishchenko, Arno Bakker
 *  Copyright 2009-2012 TECHNISCHE UNIVERSITEIT DELFT. All rights reserved.
 *
 */

#include "hashtree.h"
#include "bin_utils.h"
//#include <openssl/sha.h>
#include "sha1.h"
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include "compat.h"
#include "swift.h"

#include <iostream>


using namespace swift;


/**     0  H a s h   t r e e       */


BulkHashTree::BulkHashTree (Storage *storage, const Sha1Hash& root_hash, uint32_t chunk_size) :
 HashTree(), root_hash_(root_hash), size_(0), sizec_(0), complete_(0), completec_(0),
 chunk_size_(chunk_size), storage_(storage), file_size_set_(false)
{
    // MULTIFILE
    storage_->SetHashTree(this);

    if (!SubmitIfPresent()) {
        dprintf("%s bulk hashtree i am leecher\n",tintstr() );
        return;
    }
}

bool BulkHashTree::SubmitIfPresent()
{
    int64_t ret = storage_->GetReservedSize();
    if (ret < 0)
        return false;

    size_ = ret;
    sizec_ = (size_ + chunk_size_-1) / chunk_size_;

    for (uint64_t i=0; i<sizec_; i++) {
	bin_t pos(0,i);
        ack_out_.set(pos);
    }
    complete_ = size_;
    completec_ = sizec_;

    return true;
}


/** In an unchecked bulk transfer we pass the HAVE chunk addresses to this */
bool            BulkHashTree::OfferPeakHash (bin_t pos, const Sha1Hash& hash) {
    char bin_name_buf[32];
    dprintf("%s bulk hashtree offer have %s\n",tintstr(),pos.str(bin_name_buf));

    if (file_size_set_)
	return true;

    if (pos == bin_t::ALL)
    {
	// Signal that all HAVEs are in.

	// ARNOTODO: win32: this is pretty slow for ~200 MB already. Perhaps do
	// on-demand sizing for Win32?
	uint64_t cur_size = storage_->GetReservedSize();
	if ( cur_size<=(sizec_-1)*chunk_size_ || cur_size>sizec_*chunk_size_ ) {
	    dprintf("%s bulk hashtree offer have resizing file\n",tintstr() );
	    if (storage_->ResizeReserved(size_)) {
		print_error("cannot set file size\n");
		SetBroken();
		return false;
	    }
	}
	file_size_set_ = true;
	return true;
    }

    // Adjust size-in-chunks based on incoming HAVEs
    uint64_t newsizec = pos.base_right().layer_offset()+1;
    if (newsizec > sizec_)
    {
	sizec_ = newsizec;
	size_ = sizec_ * chunk_size_;
    }

    return true;
}


const Sha1Hash& BulkHashTree::peak_hash (int i) const
{
    return Sha1Hash::ZERO;
}

const Sha1Hash& BulkHashTree::hash (bin_t pos) const
{
    return Sha1Hash::ZERO;
}

bin_t         BulkHashTree::peak_for (bin_t pos) const
{
    return bin_t::NONE;
}

bool            BulkHashTree::OfferHash (bin_t pos, const Sha1Hash& hash)
{
    return OfferPeakHash(pos,hash);
}


bool            BulkHashTree::OfferData (bin_t pos, const char* data, size_t length)
{
    if (!size())
        return false;
    if (!pos.is_base())
        return false;
    if (length<chunk_size_ && pos!=bin_t(0,sizec_-1))
        return false;
    if (ack_out_.is_filled(pos))
        return true; // to set data_in_

    //printf("g %lli %s\n",(uint64_t)pos,hash.hex().c_str());
    ack_out_.set(pos);
    // Arno,2011-10-03: appease g++
    if (storage_->Write(data,length,pos.base_offset()*chunk_size_) < 0)
        print_error("pwrite failed");
    complete_ += length;
    completec_++;
    if (pos.base_offset()==sizec_-1) {
        size_ = ((sizec_-1)*chunk_size_) + length;
        if (storage_->GetReservedSize()!=size_)
            storage_->ResizeReserved(size_);
    }
    return true;

}


uint64_t      BulkHashTree::seq_complete (int64_t offset)
{
    return size_;
}


BulkHashTree::~BulkHashTree ()
{
}

