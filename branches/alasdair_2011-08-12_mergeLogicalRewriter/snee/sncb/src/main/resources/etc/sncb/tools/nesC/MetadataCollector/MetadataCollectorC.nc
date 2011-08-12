/*
 * Copyright (c) 2006 Intel Corporation
 * All rights reserved.
 *
 * This file is distributed under the terms in the attached INTEL-LICENSE     
 * file. If you do not find these files, copies can be found by writing to
 * Intel Research Berkeley, 2150 Shattuck Avenue, Suite 1300, Berkeley, CA, 
 * 94704.  Attention:  Intel License Inquiry.
 */

/**
 * MultihopOscilloscope demo application using the collection layer. 
 * See README.txt file in this directory and TEP 119: Collection.
 *
 * @author David Gay
 * @author Kyle Jamieson
 */

#include "Timer.h"
#include "MetadataCollector.h"
#include "CommandServer.h"

module MetadataCollectorC @safe(){
  uses {
    // Interfaces for initialization:
    interface Boot;
    interface SplitControl as RadioControl;
    interface StdControl as RoutingControl;
    
    // Interfaces for communication, multihop and serial:
    interface Send;
    interface Receive;
    interface AMSend as SerialSend;
    interface CollectionPacket;
    interface RootControl;
    interface CtpInfo;

    interface Queue<message_t *> as UARTQueue;
    interface Pool<message_t> as UARTMessagePool;

    // Miscalleny:
    interface Timer<TMilli>;
    interface Leds;

    interface SplitControl as CommandServer;
    interface StateChanged;

    interface Read<uint16_t>;
  }
}

implementation {
  task void uartSendTask();
  static void startTimer();
  static void fatal_problem();
  static void report_problem();
  static void report_sent();
  static void report_received();

  uint8_t uartlen;
  message_t sendbuf;
  message_t uartbuf;
  bool sendbusy=FALSE, uartbusy=FALSE;

  /* Current local state - interval, version and accumulated readings */
  oscilloscope_t local;

  bool radioOn = FALSE;

  // 
  // On bootup, initialize radio and serial communications, and our
  // own state variables.
  //
  event void Boot.booted() {
    local.id = TOS_NODE_ID;
    call CommandServer.start();
  }

  event void RadioControl.startDone(error_t error) {
    if (error != SUCCESS)
      fatal_problem();
    
    radioOn = TRUE;

    if (sizeof(local) > call Send.maxPayloadLength())
      fatal_problem();
  }

  event void RadioControl.stopDone(error_t error) { }

  static void startTimer() {
    if (call Timer.isRunning()) call Timer.stop();
    call Timer.startPeriodic(DEFAULT_INTERVAL);
  }

  //
  // Only the root will receive messages from this interface; its job
  // is to forward them to the serial uart for processing on the pc
  // connected to the sensor network.
  //
  event message_t*
  Receive.receive(message_t* msg, void *payload, uint8_t len) {
    oscilloscope_t* in = (oscilloscope_t*)payload;
    oscilloscope_t* out;
    if (uartbusy == FALSE) {
      out = (oscilloscope_t*)call SerialSend.getPayload(&uartbuf, sizeof(oscilloscope_t));
      if (len != sizeof(oscilloscope_t) || out == NULL) {
	return msg;
      }
      else {
	memcpy(out, in, sizeof(oscilloscope_t));
      }
      uartlen = sizeof(oscilloscope_t);
      post uartSendTask();
    } else {
      // The UART is busy; queue up messages and service them when the
      // UART becomes free.
      message_t *newmsg = call UARTMessagePool.get();
      if (newmsg == NULL) {
        // drop the message on the floor if we run out of queue space.
        report_problem();
        return msg;
      }

      //Serial port busy, so enqueue.
      out = (oscilloscope_t*)call SerialSend.getPayload(newmsg, sizeof(oscilloscope_t));
      if (out == NULL) {
	return msg;
      }
      memcpy(out, in, sizeof(oscilloscope_t));

      if (call UARTQueue.enqueue(newmsg) != SUCCESS) {
        // drop the message on the floor and hang if we run out of
        // queue space without running out of queue space first (this
        // should not occur).
        call UARTMessagePool.put(newmsg);
        fatal_problem();
        return msg;
      }
    }

    return msg;
  }

  task void uartSendTask() {
    if (call SerialSend.send(0xffff, &uartbuf, uartlen) != SUCCESS) {
      report_problem();
    } else {
      uartbusy = TRUE;
    }
  }

  event void SerialSend.sendDone(message_t *msg, error_t error) {
    uartbusy = FALSE;
    if (call UARTQueue.empty() == FALSE) {
      // We just finished a UART send, and the uart queue is
      // non-empty.  Let's start a new one.
      message_t *queuemsg = call UARTQueue.dequeue();
      if (queuemsg == NULL) {
        fatal_problem();
        return;
      }
      memcpy(&uartbuf, queuemsg, sizeof(message_t));
      if (call UARTMessagePool.put(queuemsg) != SUCCESS) {
        fatal_problem();
        return;
      }
      post uartSendTask();
    }
  }

  /* At each sample period:
     - if local sample buffer is full, send accumulated samples
     - read next sample
  */
  event void Timer.fired() {    
    call Read.read();
  }

  event void Send.sendDone(message_t* msg, error_t error) {
    if (error == SUCCESS)
      report_sent();
    else
      report_problem();

    sendbusy = FALSE;
  }

  // Use LEDs to report various status issues.
  static void fatal_problem() { 
    call Leds.led0On(); 
    call Leds.led1On();
    call Leds.led2On();
    call Timer.stop();
  }

  static void report_problem() { call Leds.led0Toggle(); }
  static void report_sent() { call Leds.led1Toggle(); }
  static void report_received() { call Leds.led2Toggle(); }

  event void CommandServer.startDone(error_t error) { }
  event void CommandServer.stopDone(error_t error) { }

  event void StateChanged.changed(uint8_t state) {
    if (state == START) {
      if (!radioOn) {
        // Beginning our initialization phases:
        if (call RadioControl.start() != SUCCESS)
          fatal_problem();        
      }

      if (call RoutingControl.start() != SUCCESS)
        fatal_problem();

      // This is how to set yourself as a root to the collection layer:
      if (local.id == 1)
        call RootControl.setRoot();

      startTimer();
    } else if (state == STOP) {
      call Timer.stop();
      call RoutingControl.stop();      
    }
  }

  event void Read.readDone(error_t result, uint16_t data) {
    uint8_t i = 0;

      if (!sendbusy) {
	oscilloscope_t *o = (oscilloscope_t *)call Send.getPayload(&sendbuf, sizeof(oscilloscope_t));
	if (o == NULL) {
	  fatal_problem();
	  return;
	}

    local.voltage = data;
       
    for (i=0; i<NREADINGS; i++) {      
      local.readings[i].id = call CtpInfo.getNeighborAddr(i);
      local.readings[i].quality = call CtpInfo.getNeighborLinkQuality(i);      
    } 

	memcpy(o, &local, sizeof(local));
	if (call Send.send(&sendbuf, sizeof(local)) == SUCCESS)
	  sendbusy = TRUE;
        else
          report_problem();
      }
  }
}
