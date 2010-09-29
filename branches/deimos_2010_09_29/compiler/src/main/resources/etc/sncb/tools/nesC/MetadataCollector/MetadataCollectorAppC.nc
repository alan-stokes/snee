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

configuration MetadataCollectorAppC { }
implementation {
  components MainC, MetadataCollectorC, new TimerMilliC(); 
  components LedsC;

  MetadataCollectorC.Boot -> MainC;
  MetadataCollectorC.Timer -> TimerMilliC;
  MetadataCollectorC.Leds -> LedsC;

  //
  // Communication components.  These are documented in TEP 113:
  // Serial Communication, and TEP 119: Collection.
  //
  components CollectionC as Collector,  // Collection layer
    ActiveMessageC,                         // AM layer
    new CollectionSenderC(AM_OSCILLOSCOPE), // Sends multihop RF
    SerialActiveMessageC,                   // Serial messaging
    new SerialAMSenderC(AM_OSCILLOSCOPE);   // Sends to the serial port

  MetadataCollectorC.RadioControl -> ActiveMessageC;
  MetadataCollectorC.RoutingControl -> Collector;

  MetadataCollectorC.Send -> CollectionSenderC;
  MetadataCollectorC.SerialSend -> SerialAMSenderC.AMSend;
  MetadataCollectorC.Receive -> Collector.Receive[AM_OSCILLOSCOPE];
  MetadataCollectorC.RootControl -> Collector;
  MetadataCollectorC.CtpInfo -> Collector;

  components new PoolC(message_t, 10) as UARTMessagePoolP,
    new QueueC(message_t*, 10) as UARTQueueP;

  MetadataCollectorC.UARTMessagePool -> UARTMessagePoolP;
  MetadataCollectorC.UARTQueue -> UARTQueueP;
  
  components CommandServerAppC;
  MetadataCollectorC.CommandServer -> CommandServerAppC.SplitControl;
  MetadataCollectorC.StateChanged -> CommandServerAppC.StateChanged;

  components SerialStarterC;
}
