#ifndef NETWORK_STATE_H
#define NETWORK_STATE_H

enum {
  START = 0x0,
  STOP = 0x1,
  REFLASH_IMAGE = 0x2,
  NETWORK_FAILURE = 0x3,
};

typedef uint8_t network_state_t;

#endif
