COMPONENT=../MetadataCollector/MetadataCollectorAppC
BOOTLOADER=tosboot

CFLAGS += -DCOMMAND_SERVER_BASESTATION
CFLAGS += -DTOSH_DATA_LENGTH=52
CFLAGS += -DSNEE_IMAGE_ID=0
CFLAGS += -I../MetadataCollector/
CFLAGS += -I../CommandServer/
CFLAGS += -I$(TOSDIR)/lib/net/ -I$(TOSDIR)/lib/net/ctp -I$(TOSDIR)/lib/net/4bitle

include $(MAKERULES)
