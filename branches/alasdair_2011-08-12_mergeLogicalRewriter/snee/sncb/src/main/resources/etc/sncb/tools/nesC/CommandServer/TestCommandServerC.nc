#include "CommandServer.h"
#include "NetworkState.h"

module TestCommandServerC {
  uses {
    interface SplitControl;
    interface NetworkState;
    interface Boot;
    interface Leds;
  }
}
implementation {

  event void Boot.booted() {
    call SplitControl.start();
  }

  event void SplitControl.startDone(error_t error) { }
    
  event void SplitControl.stopDone(error_t error) { }

  event void NetworkState.changed(network_state_t state) {
    if (state == START) {
      call Leds.led0On();
    } else if (state == STOP) {
      call Leds.led0Off();
    }
  }
}
