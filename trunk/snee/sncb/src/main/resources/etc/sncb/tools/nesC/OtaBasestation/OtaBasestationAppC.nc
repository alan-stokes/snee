#include "OtaBasestation.h"

configuration OtaBasestationAppC {}
implementation {
  components OtaBasestationC as App;
  components MainC;
  App.Boot -> MainC.Boot;

  components LedsC;
  App.Leds -> LedsC;

  // auto-start serial stack
  components SerialStarterC;
  // serial wirings
  components new SerialAMSenderC(0x53) as SerialAMSender;
  components new SerialAMReceiverC(0x53) as SerialAMReceiver;	  
  App.SerialAMSender -> SerialAMSender;
  App.SerialAMReceiver -> SerialAMReceiver;

  // DYMO
  components DymoNetworkC;
  App.MultiHopSplitControl -> DymoNetworkC;
  App.Packet -> DymoNetworkC;
  App.MultiHopPacket -> DymoNetworkC;
  App.MultiHopReceive -> DymoNetworkC.Receive[DYMO_OTA_PACKET];
  App.MultiHopSend -> DymoNetworkC.MHSend[DYMO_OTA_PACKET];

  // Node Controller
  components CommandServerAppC;
  App.CommandServer -> CommandServerAppC.SplitControl;
  App.NetworkState -> CommandServerAppC.NetworkState;

  components new TimerMilliC();
  App.TimeoutTimer -> TimerMilliC;
}
