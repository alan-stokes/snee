COMPONENT=OtaServerAppC
BOOTLOADER=tosboot

CFLAGS += -DSNEE_IMAGE_ID=1
CFLAGS += -DTOSH_DATA_LENGTH=40
CFLAGS += -I$(TOSDIR)/lib/net/tymo -I$(TOSDIR)/lib/net/tymo/dymo -I$(TOSDIR)/lib/net/tymo/mh
CFLAGS += -I../../CommandServer

include $(MAKERULES)
