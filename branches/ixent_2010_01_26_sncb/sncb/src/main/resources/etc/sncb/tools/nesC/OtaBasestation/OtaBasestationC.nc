#include <Timer.h>
#include "OtaBasestation.h"

module OtaBasestationC {
  uses {    
    interface Boot;
    interface Leds;    

    interface SplitControl as MultiHopSplitControl;
    interface AMPacket as MultiHopPacket;
    interface Packet;
    interface Receive as MultiHopReceive;
    interface AMSend as MultiHopSend;

    interface AMSend as SerialAMSender;
    interface Receive as SerialAMReceiver;
    
    interface SplitControl as CommandServer;
    interface NetworkState;
		
		interface Timer<TMilli> as TimeoutTimer;
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

  void sendSerialReply(error_t error, void* payload, int len) {
		// Get a buffer for serial comms
		SerialReplyPacket* serialPayload = (SerialReplyPacket*) call SerialAMSender.getPayload(&serialPacket, sizeof(SerialReplyPacket));

		if (serialPayload == NULL) {
			programFailure();
		}

		if (call SerialAMSender.maxPayloadLength() < sizeof(SerialReplyPacket)) { 
			programFailure();
		}

		if (error == SUCCESS) {
			// Is len >= sizeof(SerialReplyPacket)? 
			memcpy(serialPayload, payload, len);
		} else {
			serialPayload->error = error;
			len = sizeof(uint8_t);
		}

		if (call SerialAMSender.send(AM_BROADCAST_ADDR, &serialPacket, len) != SUCCESS) {
			programFailure();							
		}		
  }

	event void SerialAMSender.sendDone(message_t* msg, error_t error) { }

  event void Boot.booted() {		
    call MultiHopSplitControl.start();
  }

  event void MultiHopSplitControl.startDone(error_t error) {		
		if (error != SUCCESS) {
			programFailure();
		}
    call CommandServer.start();
	}

  event void MultiHopSplitControl.stopDone(error_t error) { }

  event void CommandServer.startDone(error_t error) {
    if (error != SUCCESS) {
			programFailure();
		}
  }

  event void CommandServer.stopDone(error_t error) { }

  event message_t* SerialAMReceiver.receive(message_t* msg, void* payload, uint8_t len) {
		error_t result;

		if (radioStackLocked) {
			programFailure();
		} else {
			SerialReqPacket* request = (SerialReqPacket*)payload;
			ota_data_msg_t* radioPayload = (ota_data_msg_t*) call Packet.getPayload(&radioPacket, sizeof(ota_data_msg_t));
		
			if (radioPayload == NULL) {
				programFailure();
				return msg;
			}

			if (call Packet.maxPayloadLength() < sizeof(ota_data_msg_t)) {
				programFailure();				
				return msg;
			}

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
			call TimeoutTimer.startOneShot(TIMEOUT);
    } else if (error == FAIL) {
// couldn't find a route
			sendSerialReply(E_NO_ROUTE, NULL, 0);
    } else {
      // gone wonky here.
      programFailure();
    }		
    radioStackLocked = FALSE;
  }

	event void TimeoutTimer.fired() {
		sendSerialReply(E_NO_REPLY, NULL, 0);
	}

	event message_t* MultiHopReceive.receive(message_t* msg, void* payload, uint8_t len) {
		// TimeoutTimer should be still running otherwise we have a problem...
		if (call TimeoutTimer.isRunning()) {
			call TimeoutTimer.stop();
		} else {
			programFailure();
		}

		sendSerialReply(SUCCESS, payload, len);

		return msg;
	}

  event void NetworkState.changed(uint8_t state) { }
}
