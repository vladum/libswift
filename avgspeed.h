/*
 *  avgspeed.h
 *  Class to compute moving average speed
 *
 *  Created by Arno Bakker
 *  Copyright 2009-2012 Delft University of Technology. All rights reserved.
 *
 */
#include "compat.h"

#ifndef AVGSPEED_H
#define AVGSPEED_H

namespace swift {


class MovingAverageSpeed
{
    public: 
        MovingAverageSpeed( tint speed_interval = 1 * TINT_SEC, tint fudge = TINT_SEC );
	void AddPoint( uint64_t amount );
        double GetSpeed();
        double GetSpeedNeutral();
        void Reset();
    protected:
        tint   speed_interval_;
        tint   t_start_;
        tint   t_end_;
        double speed_;
        tint   fudge_;
        bool   resetstate_;
};

}

#endif
