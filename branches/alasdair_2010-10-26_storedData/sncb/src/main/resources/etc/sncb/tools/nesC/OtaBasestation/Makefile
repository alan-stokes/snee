COMPONENT=OtaBasestationAppC
BOOTLOADER=tosboot

CFLAGS += -DTOSH_DATA_LENGTH=40
CFLAGS += -DCOMMAND_SERVER_BASESTATION
CFLAGS += -I../CommandServer
CFLAGS += -I$(TOSDIR)/lib/net/ -I$(TOSDIR)/lib/net/drip
CFLAGS += -I../tymo -I../tymo/dymo -I../tymo/mh

include $(MAKERULES)

