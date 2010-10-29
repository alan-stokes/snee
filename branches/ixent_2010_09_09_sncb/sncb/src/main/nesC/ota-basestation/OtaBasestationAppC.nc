#include "OtaBasestation.h"
#include "Deluge.h"

configuration OtaBasestationAppC {}
implementation {
  components OtaBasestationC as App, LedsC, MainC;
  components SerialActiveMessageC;

  App.Boot -> MainC.Boot;
	App.Leds -> LedsC;

	App.SplitControl -> SerialActiveMessageC;

// Flash manager wiring
  components new SerialAMSenderC(DELUGE_AM_FLASH_VOL_MANAGER) as FlashManagerSender;
  components new SerialAMReceiverC(DELUGE_AM_FLASH_VOL_MANAGER) as FlashManagerReceiver;	  
  App.FlashManagerSender -> FlashManagerSender;
  App.FlashManagerReceiver -> FlashManagerReceiver;

	components DymoNetworkC;

  App.SplitControl -> DymoNetworkC;
  App.MultiHopReceive -> DymoNetworkC.Receive[DYMO_OTA_PACKET];
  App.MultiHopSend -> DymoNetworkC.MHSend[DYMO_OTA_PACKET];
	App.MultiHopPacket -> DymoNetworkC;
	App.RadioPacket -> DymoNetworkC;
}


