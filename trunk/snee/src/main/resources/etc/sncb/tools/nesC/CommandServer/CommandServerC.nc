/*
 * Copyright (c) 2006 Arched Rock Corporation
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the
 *   distribution.
 * - Neither the name of the Arched Rock Corporation nor the names of
 *   its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
 * ARCHED ROCK OR ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE
 *
 */

#include <Timer.h>
#include "CommandServer.h"
#include "NetworkState.h"

module CommandServerC {
  provides {
    interface SplitControl;
    interface NetworkState;
  }
  uses {
    interface Boot;
	
    interface SplitControl as RadioControl;

    interface StdControl as DisseminationControl;

    interface DisseminationValue<command_msg_t>;
    interface DisseminationUpdate<command_msg_t>;

    interface Leds;

    interface Timer<TMilli>;

    interface NetProg;

#ifdef COMMAND_SERVER_BASESTATION
    interface Receive;
#endif
  }
}
implementation {

  uint32_t newImageNum;

  task void signalStopDone();

  event void Boot.booted() {
    command_msg_t initialCmd;
    initialCmd.cmd = 0xFF;
    initialCmd.imageNum = 0xFF;
    call DisseminationValue.set(&initialCmd); 
  }

  command error_t SplitControl.start() {
    return call RadioControl.start();
  }

  command error_t SplitControl.stop() {
    post signalStopDone();
    return SUCCESS;
  }

  task void signalStopDone() {
    signal SplitControl.stopDone(SUCCESS);
  }

  event void RadioControl.startDone( error_t result ) {    
    call DisseminationControl.start();
    signal SplitControl.startDone(SUCCESS);      
  }

  event void RadioControl.stopDone( error_t result ) {
    signal SplitControl.stopDone(SUCCESS);
  }

#ifdef COMMAND_SERVER_BASESTATION
  event message_t* Receive.receive(message_t* msg, void* payload, uint8_t len) {        
    serial_msg_t* serial_msg;
    command_msg_t cmd;

	  serial_msg = (serial_msg_t*)payload;
    cmd.cmd = serial_msg->cmd;
    cmd.imageNum = serial_msg->imageNum;

    call DisseminationUpdate.change(&cmd);

    return msg;
  }
#endif

  event void DisseminationValue.changed() {
    const command_msg_t* newCmd = call DisseminationValue.get();

    if (newCmd->cmd == REFLASH_IMAGE) {
// Don't reboot the basestation!
#ifndef COMMAND_SERVER_BASESTATION      
// Prevent reflashing of current image. This could probably be improved.
      if (SNEE_IMAGE_ID == newCmd->imageNum) { return; }

      switch (newCmd->imageNum) {
        case 0:
          call Leds.led0On();
          newImageNum = SLOT_1;
          break;
        case 1:
          call Leds.led1On();
          newImageNum = SLOT_2;
          break;
        case 2:
          call Leds.led2On();
          newImageNum = SLOT_3;
          break;
        default:
          call Leds.led0On();
          call Leds.led1On();
          call Leds.led2On();
          break;
      }

      call Timer.startOneShot(REBOOT_TIME);		
#endif
    } else {
// Everybody hears all the other commands
      signal NetworkState.changed(newCmd->cmd);
    }
  }

  event void Timer.fired() {
      switch (newImageNum) {
        case 0:
          call Leds.led0Off();
          break;
        case 1:
          call Leds.led1Off();
          break;
        case 2:
          call Leds.led2Off();
          break;
        default:
          call Leds.led0Off();
          call Leds.led1Off();
          call Leds.led2Off();
          break;
      }

// Call programImageAndReboot() with the image address rather than use 
// StorageMap, this saves about 4K. If the image boundaries change you will need
// to redefine the address in CommandServer.h
    call NetProg.programImageAndReboot(newImageNum);
  }

  command void NetworkState.setNetworkFailureState() {
    command_msg_t cmd;
    cmd.cmd = NETWORK_FAILURE;
    call DisseminationUpdate.change(&cmd);
  }
}
