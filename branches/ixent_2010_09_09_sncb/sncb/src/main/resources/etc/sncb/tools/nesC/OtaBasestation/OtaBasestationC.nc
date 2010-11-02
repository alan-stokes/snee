#include <Timer.h>
#include "OtaBasestation.h"

module OtaBasestationC {
  uses {
    interface SplitControl;
    interface Leds;
    interface Boot;

    interface AMSend as FlashManagerSender;
    interface Receive as FlashManagerReceiver;

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
	bool serialStackLocked = FALSE;

  SerialReplyPacket* serialPayload;

	void programFailure() {
		call Leds.led0On();
		call Leds.led1On();
		call Leds.led2On();
	}

  event void Boot.booted() {		
// Start the node controller
    call SplitControl.start();

// Get a buffer for serial comms
    serialPayload = (SerialReplyPacket*) call FlashManagerSender.getPayload(&serialPacket, sizeof(SerialReplyPacket));

    if (serialPayload == NULL) {
      programFailure();
    }

    if (call FlashManagerSender.maxPayloadLength() < sizeof(SerialReplyPacket)) { 
      programFailure();
    }
  }

  event void SplitControl.startDone(error_t error) {		
		if (error != SUCCESS) {
			programFailure();
		}
	}

  event void SplitControl.stopDone(error_t error) { }

  event message_t* FlashManagerReceiver.receive(message_t* msg, void* payload, uint8_t len) {
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
    } else if (error == FAIL) {
// couldn't find a route
		  if (!serialStackLocked) {
        serialStackLocked = TRUE;

			  serialPayload->error = ERROR_FAIL;

  		  if (call FlashManagerSender.send(AM_BROADCAST_ADDR, &serialPacket, sizeof(SerialReplyPacket)) != SUCCESS) {				
			  	serialStackLocked = FALSE;
			  }
		  }    
    } else {
      // gone wonky here.
      programFailure();
    }		
    radioStackLocked = FALSE;
  }

	event message_t* MultiHopReceive.receive(message_t* msg, void* payload, uint8_t len) {
    // Lock the serial stack. The serial stack shouldn't be locked at this
    // point but check anyway
		if (!serialStackLocked) {
      serialStackLocked = TRUE;

      // Is len >= sizeof(SerialReplyPacket)? 
			memcpy(serialPayload, payload, len);

  		if (call FlashManagerSender.send(AM_BROADCAST_ADDR, &serialPacket, len) != SUCCESS) {				
				serialStackLocked = FALSE;
			}
		}

		return msg;
	}

	event void FlashManagerSender.sendDone(message_t* msg, error_t error) {
		serialStackLocked = FALSE;	
	}

  event void CommandServer.startDone(error_t error) { }
  event void CommandServer.stopDone(error_t error) { }
  event void StateChanged.changed(uint8_t state) { }

  event void Timer.fired() {
  }
}
