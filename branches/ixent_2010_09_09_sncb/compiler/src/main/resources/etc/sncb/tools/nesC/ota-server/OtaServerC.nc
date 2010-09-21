#include "Timer.h"

module OtaServerC
{
  uses interface Boot;
  uses interface SplitControl;
  uses interface SplitControl as CommandServer;
}

implementation
{
  event void Boot.booted() {
    call SplitControl.start();
  }

  event void SplitControl.startDone(error_t error) { }
  event void SplitControl.stopDone(error_t error) { }

  event void CommandServer.startDone(error_t error) { }
  event void CommandServer.stopDone(error_t error) { }
}

