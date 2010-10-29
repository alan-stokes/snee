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

// Timer delay for reprogramming
#define REBOOT_TIME 10 * 1024

// AM id for serial 
#define SNEE_OTA_MANAGER 0x1

// Memory addresses of slots in external flash for program images sent over-the
// -air. Yes, I know about StorageMap.
enum {
  SLOT_1 = 983040,
  SLOT_2 = 0,
  SLOT_3 = 65536,
  SLOT_4 = 131072,
};


#endif
