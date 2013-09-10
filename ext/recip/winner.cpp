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

tint WinnerReciprocityPolicy::SendIntervalFor(Channel *channel) {
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
            desired_ratio = 0.0;
        else if (channel->id() == 2)
            desired_ratio = 1.0;
    }
    double error = (desired_ratio - feedback_ratio);
    double p_coef = 0.8;
    double ratio = feedback_ratio + error * p_coef;
    printf("\t\tdesired ratio: %f ratio: %lf\n", desired_ratio, ratio);

    if (ratio == 0.0)
        return TINT_NEVER;
    else
        return TINT_SEC / (rate * ratio);
}
