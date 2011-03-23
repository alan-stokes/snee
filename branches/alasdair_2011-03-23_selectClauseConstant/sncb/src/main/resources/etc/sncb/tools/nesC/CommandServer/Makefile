COMPONENT=TestCommandServerAppC
BOOTLOADER=tosboot

CFLAGS += -DSNEE_IMAGE_ID=2
#CFLAGS += -DCOMMAND_SERVER_BASESTATION

### For Drip:
CFLAGS += -I$(TOSDIR)/lib/net -I%T/lib/net/drip

### For DIP:
#CFLAGS += -I$(TOSDIR)/lib/net -I%T/lib/net/dip -I%T/lib/net/dip/interfaces

### For Dhv
#CFLAGS += -I$(TOSDIR)/lib/net -I%T/lib/net/dhv -I%T/lib/net/dhv/interfaces

include $(MAKERULES)

