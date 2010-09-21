#ifndef COMMAND_SERVER_H
#define COMMAND_SERVER_H

enum {
  START = 0x0,
  STOP = 0x1,
  REFLASH_IMAGE = 0x2,
};

typedef nx_struct serial_msg_t {
	nx_uint8_t cmd;
	nx_uint8_t imageNum;
} serial_msg_t;

typedef nx_struct command_msg_t {
    nx_uint8_t cmd;
    nx_uint8_t imageNum;
} command_msg_t;

#define REBOOT_TIME 10 * 1024

#endif
