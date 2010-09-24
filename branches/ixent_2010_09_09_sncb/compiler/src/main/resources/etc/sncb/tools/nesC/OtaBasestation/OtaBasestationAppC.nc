#include "OtaBasestation.h"

configuration OtaBasestationAppC {}
implementation {
  components OtaBasestationC as App, MainC;
  components SerialActiveMessageC;
//  components LedsC;

  App.Boot -> MainC.Boot;
//  App.Leds -> LedsC;
  App.SplitControl -> SerialActiveMessageC;

// Flash manager wiring
  components new SerialAMSenderC(0x53) as FlashManagerSender;
  components new SerialAMReceiverC(0x53) as FlashManagerReceiver;	  
  App.FlashManagerSender -> FlashManagerSender;
  App.FlashManagerReceiver -> FlashManagerReceiver;

  components DymoNetworkC;
  App.SplitControl -> DymoNetworkC;
  App.MultiHopReceive -> DymoNetworkC.Receive[DYMO_OTA_PACKET];
  App.MultiHopSend -> DymoNetworkC.MHSend[DYMO_OTA_PACKET];
  App.MultiHopPacket -> DymoNetworkC;
  App.RadioPacket -> DymoNetworkC;

  components CommandServerAppC;
  App.CommandServer -> CommandServerAppC.SplitControl;
  App.StateChanged -> CommandServerAppC.StateChanged;

  components SerialStarterC;
}


