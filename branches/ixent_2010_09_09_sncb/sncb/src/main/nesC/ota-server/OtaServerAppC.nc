#include "AM.h"
#include "StorageVolumes.h"

#include "BlockStorageManager.h"
#include "Deluge.h"

configuration OtaServerAppC { }

implementation
{
// General stuff
  components MainC, OtaServerC;

  OtaServerC -> MainC.Boot;

// Flash volume manager stuff
  components new FlashVolumeManagerP();
  components new TimerMilliC() as TimeoutTimer;
  components NoLedsC, LedsC;
  components BlockStorageLockC;
  components new BlockStorageLockClientC();

  components new BlockReaderC(VOLUME_GOLDENIMAGE) as BlockReaderGoldenImage;
  components new BlockReaderC(VOLUME_DELUGE1)     as BlockReaderDeluge1;
  components new BlockReaderC(VOLUME_DELUGE2)     as BlockReaderDeluge2;
  components new BlockReaderC(VOLUME_DELUGE3)     as BlockReaderDeluge3;

  components new BlockWriterC(VOLUME_GOLDENIMAGE) as BlockWriterGoldenImage;
  components new BlockWriterC(VOLUME_DELUGE1)     as BlockWriterDeluge1;
  components new BlockWriterC(VOLUME_DELUGE2)     as BlockWriterDeluge2;
  components new BlockWriterC(VOLUME_DELUGE3)     as BlockWriterDeluge3;
  
  FlashVolumeManagerP.BlockRead[VOLUME_GOLDENIMAGE] -> BlockReaderGoldenImage;
  FlashVolumeManagerP.BlockRead[VOLUME_DELUGE1]     -> BlockReaderDeluge1;
  FlashVolumeManagerP.BlockRead[VOLUME_DELUGE2]     -> BlockReaderDeluge2;
  FlashVolumeManagerP.BlockRead[VOLUME_DELUGE3]     -> BlockReaderDeluge3;

  FlashVolumeManagerP.BlockWrite[VOLUME_GOLDENIMAGE] -> BlockWriterGoldenImage;
  FlashVolumeManagerP.BlockWrite[VOLUME_DELUGE1]     -> BlockWriterDeluge1;
  FlashVolumeManagerP.BlockWrite[VOLUME_DELUGE2]     -> BlockWriterDeluge2;
  FlashVolumeManagerP.BlockWrite[VOLUME_DELUGE3]     -> BlockWriterDeluge3;

  FlashVolumeManagerP.Resource -> BlockStorageLockClientC;
  FlashVolumeManagerP.ArbiterInfo -> BlockStorageLockC;
 
  FlashVolumeManagerP.TimeoutTimer -> TimeoutTimer;
  FlashVolumeManagerP.Leds -> LedsC;

// DYMO stuff
  components DymoNetworkC;
  OtaServerC.SplitControl -> DymoNetworkC;
  FlashVolumeManagerP.MultiHopReceive -> DymoNetworkC.Receive[1];
  FlashVolumeManagerP.MultiHopSend -> DymoNetworkC.MHSend[1];
  FlashVolumeManagerP.MultiHopPacket -> DymoNetworkC;
  FlashVolumeManagerP.RadioPacket -> DymoNetworkC;

// NetProg stuff -- seperate this out into seperate components?
  components NetProgC;
  components new TimerMilliC() as Timer;
  components BlockStorageManagerC;
  FlashVolumeManagerP.NetProg -> NetProgC;
  FlashVolumeManagerP.DelayTimer -> Timer;
  FlashVolumeManagerP.StorageMap -> BlockStorageManagerC;

// Command server stuff
  components TestDisseminationAppC;
  OtaServerC.CommandServer -> TestDisseminationAppC.CommandServer;
  FlashVolumeManagerP.StateChanged -> TestDisseminationAppC.StateChanged;
}

