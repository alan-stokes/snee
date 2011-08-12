// __OPERATOR_DESCRIPTION__
// This module reads tuple from a buffer and sends them over the radio.

__HEADER__

	command error_t DoTask.doTask()
  	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ doTask() entered, now listen to radio and place tuples in trays\n");

	   	return SUCCESS;
  	}

	command error_t DoTask.open()
	{
		//Operator that reads from the tray will call tray open()
		return SUCCESS;
	}	

	event message_t* Receive.receive(message_t* msg, void* payload, uint8_t len)
	{
		__NESC_DEBUG_LEDS__

		if (len==sizeof(__MESSAGE_TYPE__))
		{
			__MESSAGE_PTR_TYPE__ packet = (__MESSAGE_PTR_TYPE__)payload;
			call PutTuples.putPacket(packet->tuples);
		}

		return msg;
	}


}



