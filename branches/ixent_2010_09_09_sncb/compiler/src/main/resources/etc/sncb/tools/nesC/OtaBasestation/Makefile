COMPONENT=OtaBasestationAppC
BOOTLOADER=tosboot

CFLAGS += -DTOSH_DATA_LENGTH=40
CFLAGS += -DCOMMAND_SERVER_BASESTATION
CFLAGS += -I../../CommandServer
CFLAGS += -I$(TOSDIR)/lib/net/ -I$(TOSDIR)/lib/net/drip
CFLAGS += -I$(TOSDIR)/lib/net/tymo -I$(TOSDIR)/lib/net/tymo/dymo -I$(TOSDIR)/lib/net/tymo/mh

include $(MAKERULES)

