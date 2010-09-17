#ifndef OTA_BASESTATION_H
#define OTA_BASESTATION_H

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
