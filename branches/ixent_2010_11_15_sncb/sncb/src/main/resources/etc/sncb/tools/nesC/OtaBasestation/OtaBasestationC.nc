#include <Timer.h>
#include "OtaBasestation.h"

module OtaBasestationC {
  uses {
    interface SplitControl;
    interface Leds;
    interface Boot;

    interface AMSend as SerialSender;
    interface Receive as SerialReceiver;

    interface AMSend as MultiHopSend;
    interface Receive as MultiHopReceive;
    interface AMPacket as MultiHopPacket;
    interface Packet as RadioPacket;
    
    interface SplitControl as CommandServer;
    interface StateChanged;

    interface Timer<TMilli>;
  }
}
implementation {

	message_t radioPacket;
	message_t serialPacket;

  bool radioStackLocked = FALSE;

	void programFailure() {
		call Leds.led0On();
		call Leds.led1On();
		call Leds.led2On();
	}

  void sendSerialPacket(uint8_t result, void* payload, uint8_t len) {
    uint8_t packetLen;
    
    SerialReplyPacket* serialPayload = (SerialReplyPacket*) call SerialSender.getPayload(&serialPacket, sizeof(SerialReplyPacket));

    if (serialPayload == NULL) {
      programFailure();
    }

    if (call SerialSender.maxPayloadLength() < sizeof(SerialReplyPacket)) { 
      programFailure();
    }

    if (result != SUCCESS) {
      serialPayload->error = result;
      packetLen = sizeof(SerialReplyPacket);
    } else {
      memcpy(serialPayload, payload, len);
      packetLen = len;
    }

    if (call SerialSender.send(AM_BROADCAST_ADDR, &serialPacket, packetLen) != SUCCESS) {
      programFailure();
    }				
  }

  event void SerialSender.sendDone(message_t* msg, error_t error) {
	}

  event void Boot.booted() {
    // ALWAYS start DYMO first!
    call SplitControl.start();		    
  }

  event void SplitControl.startDone(error_t error) {
    if (error == SUCCESS) {
      call CommandServer.start();
    }
  }

  event void SplitControl.stopDone(error_t error) { }

  event void CommandServer.startDone(error_t error) { }

  event void CommandServer.stopDone(error_t error) { }

  event void StateChanged.changed(uint8_t state) { }

  event message_t* SerialReceiver.receive(message_t* msg, void* payload, uint8_t len) {    
		error_t result;    

		if (radioStackLocked) {
			programFailure();
		} else {
			SerialReqPacket* request = (SerialReqPacket*)payload;
			ota_data_msg_t* radioPayload = (ota_data_msg_t*) call RadioPacket.getPayload(&radioPacket, sizeof(ota_data_msg_t));
		
			if (radioPayload == NULL) {
				programFailure();
				return msg;
			}

			if (call RadioPacket.maxPayloadLength() < sizeof(ota_data_msg_t)) {
				programFailure();				
				return msg;
			}

      // DYMO inserts data into the packet so adjust the packet length
      // accordingly.
			memcpy(radioPayload->data, payload + sizeof(uint16_t), len - sizeof(uint16_t));

			result = call MultiHopSend.send(request->id, &radioPacket, len);

  		if (result == SUCCESS)	{								
				radioStackLocked = TRUE;        
  		} else {
				programFailure();
			}
		}
		
		return msg;
  }

  event void MultiHopSend.sendDone(message_t * msg, error_t error) {
    if (&radioPacket == msg && error == SUCCESS ) {
// dymo has a route and the packet has been sent.
      // Start the timeout timer
      call Timer.startOneShot(TIMEOUT);
    } else if (error == FAIL) {
// couldn't find a route
      sendSerialPacket(ERROR_NO_ROUTE, NULL, 0);
    } else {
// gone wonky here.
      programFailure();
    }		
    radioStackLocked = FALSE;
  }

	event message_t* MultiHopReceive.receive(message_t* msg, void* payload, uint8_t len) {
    // Timer should be running, otherwise we have a problem because we've
    // signalled to the basestation that we didn't get a reply packet.    
    if (call Timer.isRunning()) {
      call Timer.stop();
    }

    sendSerialPacket(SUCCESS, payload, len);
		return msg;
	}

  event void Timer.fired() {
    sendSerialPacket(ERROR_NO_REPLY, NULL, 0);
  }
}

