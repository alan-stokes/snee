/*
 * Copyright (c) 2006 Intel Corporation
 * All rights reserved.
 *
 * This file is distributed under the terms in the attached INTEL-LICENSE     
 * file. If you do not find these files, copies can be found by writing to
 * Intel Research Berkeley, 2150 Shattuck Avenue, Suite 1300, Berkeley, CA, 
 * 94704.  Attention:  Intel License Inquiry.
 */

/**
 * @author David Gay
 * @author Kyle Jamieson
 */

#ifndef MULTIHOP_OSCILLOSCOPE_H
#define MULTIHOP_OSCILLOSCOPE_H

enum {
  /* Number of readings per message. If you increase this, you may have to
     increase the message_t size. */
  NREADINGS = 4,
  /* Default sampling period. */
  DEFAULT_INTERVAL = 1024,
  AM_OSCILLOSCOPE = 0x93
};

typedef nx_struct neighbour {
  nx_uint16_t id;
  nx_uint16_t quality;
} neighbour_t;

typedef nx_struct oscilloscope {
  nx_uint16_t id; /* Mote id of sending mote. */
  neighbour_t readings[NREADINGS];
} oscilloscope_t;

#endif
