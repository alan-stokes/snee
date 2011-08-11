COMPONENT=MetadataCollectorAppC
BOOTLOADER=tosboot

CFLAGS += -DSNEE_IMAGE_ID=0
CFLAGS += -DTOSH_DATA_LENGTH=52
CFLAGS += -I../CommandServer/
CFLAGS += -I$(TOSDIR)/lib/net/ -I$(TOSDIR)/lib/net/ctp -I$(TOSDIR)/lib/net/4bitle

include $(MAKERULES)
