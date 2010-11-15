#include "CommandServer.h"

module TestCommandServerC {
  uses {
    interface SplitControl;
    interface StateChanged;
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

  event void StateChanged.changed(uint8_t state) {
    if (state == START) {
      call Leds.led0On();
    } else if (state == STOP) {
      call Leds.led0Off();
    }
  }
}
