#ifndef OTA_BASESTATION_H
#define OTA_BASESTATION_H

// copied from FlashVolumeManager.P
typedef nx_struct SerialReqPacket {
	nx_uint16_t id;	
	nx_uint8_t cmd;
	nx_uint8_t imgNum;
	nx_uint32_t offset;
	nx_uint16_t len;
	nx_uint8_t data[0];
} SerialReqPacket;
  
typedef nx_struct SerialReplyPacket {
	nx_uint8_t error;
	nx_uint8_t data[0];
} SerialReplyPacket;

nx_struct ShortIdent {
	nx_uint8_t name[16];
	nx_uint32_t timestamp;
	nx_uint32_t uidhash;
	nx_uint16_t nodeid;
};

enum {
  DYMO_OTA_PACKET = 0x1,
};

typedef nx_struct ota_data_msg_t {
	nx_uint8_t data[0];
} ota_data_msg_t;

#endif
