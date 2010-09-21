#include <Timer.h>

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

/**
 * TestDisseminationC exercises the dissemination layer, by causing
 * the node with ID 1 to inject 2 new values into the network every 4
 * seconds. For the 32-bit object with key 0x1234, node 1 Toggles LED
 * 0 when it sends, and every other node Toggles LED 0 when it
 * receives the correct value. For the 16-bit object with key 0x2345,
 * node 1 Toggles LED 1 when it sends, and every other node Toggles
 * LED 1 when it receives the correct value.
 *
 * See TEP118 - Dissemination for details.
 * 
 * @author Gilman Tolle <gtolle@archedrock.com>
 * @version $Revision: 1.6 $ $Date: 2007/04/18 04:02:06 $
 */

#include "CommandServer.h"

module CommandServerC {
  provides {
    interface SplitControl;
    interface StateChanged;
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
    interface StorageMap[uint8_t volumeId];

    interface SplitControl as SerialControl;
    interface Receive;
  }
}
implementation {

  uint8_t newImageNum;

  event void Boot.booted() {
    command_msg_t initialCmd;
    initialCmd.cmd = 0xFF;
    initialCmd.imageNum = 0xFF;
    call DisseminationValue.set(&initialCmd); 
  }

  command error_t SplitControl.start() {
    return call SerialControl.start();
  }

  command error_t SplitControl.stop() {
    //return call SerialControl.stop();
    return SUCCESS;
  }

  event void SerialControl.startDone(error_t error) {
    call RadioControl.start();
  }
  
  event void SerialControl.stopDone(error_t error) {
    call RadioControl.stop();
  }

  event void RadioControl.startDone( error_t result ) {    
    call DisseminationControl.start();
    signal SplitControl.startDone(SUCCESS);      
  }

  event void RadioControl.stopDone( error_t result ) {
    signal SplitControl.stopDone(SUCCESS);
  }

  event message_t* Receive.receive(message_t* msg, void* payload, uint8_t len) {        
    serial_msg_t* serial_msg;
    command_msg_t cmd;

	serial_msg = (serial_msg_t*)payload;
    cmd.cmd = serial_msg->cmd;
    cmd.imageNum = serial_msg->imageNum;

    call DisseminationUpdate.change(&cmd);

    return msg;
  }

  event void DisseminationValue.changed() {
    const command_msg_t* newCmd = call DisseminationValue.get();

    if (newCmd->cmd == REFLASH_IMAGE) {
// Don't reboot the basestation!
#ifndef COMMAND_SERVER_BASESTATION      
      newImageNum = newCmd->imageNum;

// Prevent reflashing of current image. This could probably be improved.
      if (SNEE_IMAGE_ID == newImageNum) { return; }

      if (newImageNum == 0) {
        call Leds.led0On();
      } else if (newImageNum == 1) {
        call Leds.led1On();
      } else if (newImageNum == 2) {
        call Leds.led2Off();
      } else {
        call Leds.led0On();
        call Leds.led1On();
        call Leds.led2On();
      }
      call Timer.startOneShot(REBOOT_TIME);		
#endif
    } else {
// Do signal start/stop to the basestation
      signal StateChanged.changed(newCmd->cmd);
    }
  }

  event void Timer.fired() {
    if (newImageNum == 0) {
      call Leds.led0Off();
    } else if (newImageNum == 1) {
      call Leds.led1Off();
    } else if (newImageNum == 2) {
      call Leds.led2Off();
    } else {
      call Leds.led0Off();
      call Leds.led1Off();
      call Leds.led2Off();
    }
    call NetProg.programImageAndReboot(call StorageMap.getPhysicalAddress[newImageNum](0));
  }
}
