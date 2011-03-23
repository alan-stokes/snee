// __MODULE_DESCRIPTION__
// This module reads tuples from one tray and sends them over the radio.

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	//vars shared for all trays (since we only process one at a time)
	int8_t bFactorCount;
	nx_int32_t evalEpoch;
	bool firstTime = TRUE;
	message_t packet;
	int8_t inHead;
	int8_t inTail;
	int8_t inQueueSize;

	int8_t tuplePacketPos;
	bool pending=FALSE;

	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	__MESSAGE_PTR_TYPE__ payload;


	void task LoopControlTask();
	void task sendPacketTask();
	void task sendPacketDoneTask();


	command error_t DoTask.open()
	{
		call GetTuples.open();
		return SUCCESS;
	}


	command error_t DoTask.doTask()
  	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ doTask() entered as evalEpoch %d, now get tuples from trays and send them on\n",evalEpoch);
		dbg("__DBG_CHANNEL__","RADIO start from __CURRENT_SITE_ID__ to __PARENT_ID__\n");

		if (firstTime) {
		   evalEpoch = 0;
		   firstTime = FALSE;
		}

		bFactorCount = 0;
		tuplePacketPos = 0;

		call GetTuples.requestData(evalEpoch);
		return SUCCESS;
	}


	event void GetTuples.requestDataDone(__TUPLE_PTR_TYPE__  _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg("__DBG_CHANNEL__", "tx:GetTuples.requestDataDone inHead=%d, inTail=%d, inQueueSize=%d\n", _inHead, _inTail, _inQueueSize);

		atomic
		{
			bFactorCount++;
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}

		post LoopControlTask();
	}


	void task LoopControlTask()
	{
		//more tuples in the inQueue
		payload = (__MESSAGE_PTR_TYPE__)(call Packet.getPayload(&packet, sizeof(__MESSAGE_TYPE__)));
		if (call Packet.maxPayloadLength() < sizeof(__MESSAGE_PTR_TYPE__))
		{
			dbg("__DBG_CHANNEL__","RADIO started but failed from __CURRENT_SITE_ID__ to __PARENT_ID__ as message size is greater\n");
			return;
      		}

		if (inHead >-1)
		{
			//generate tuples
			payload->tuples[tuplePacketPos] = inQueue[inHead];
			inHead = (inHead+1) % inQueueSize;
			if (inHead==inTail)
			{
				inHead = -1;
			}
			tuplePacketPos++;
		}

		if ((tuplePacketPos == __TUPLES_PER_PACKET__)) //current packet is full
		{
			post sendPacketTask();
		}
		else if (inHead >-1) // more tuples in inQueue
		{
			post LoopControlTask();
		}
		else if (bFactorCount < BUFFERING_FACTOR) // more data still buffered at other evaluation times
		{
			evalEpoch++;
			call GetTuples.requestData(evalEpoch);
		}

		else // no more data at other evaluation times
		{
			if (tuplePacketPos>0) // packet being constructed contains tuples, send
			{
				post sendPacketTask();
			}
			else
			{
				evalEpoch++;
				signal DoTask.doTaskDone(SUCCESS);
			}
		}
	}


	void task sendPacketTask()
	{
		if (pending)
		{
			dbg("__DBG_CHANNEL__", "Still pending");
			post sendPacketTask();
		}
		else
		{
			atomic pending = TRUE;
			dbg("__DBG_CHANNEL__","Attempting to send __MESSAGE_TYPE__ packet to __PARENT_ID__\n");

			//Pad any unsed tuples
			while (tuplePacketPos < __TUPLES_PER_PACKET__) // tuples per packet for source fragment type
			{
				payload->tuples[tuplePacketPos].evalEpoch = NULL_EVAL_EPOCH;
				tuplePacketPos++;
			}

			if (call AMSend.send(__PARENT_ID__,&packet, sizeof(__MESSAGE_TYPE__) )!=SUCCESS)
			{
				dbg("__DBG_CHANNEL__","Error: Call to active message layer failed in __MODULE_NAME__\n");
				atomic pending = FALSE;
				//post sendPacketTask(); // retry sending packet (comment out accordingly)
				post sendPacketDoneTask(); // do not retry sending packet (comment out accordingly)
			}
			else
			{
				dbg("__DBG_CHANNEL__", "Message accepted by active message layer in __MODULE_NAME__\n");
			}
		}

	}


	void task sendPacketDoneTask()
	{
		atomic pending = FALSE;
		tuplePacketPos=0;

		__NESC_DEBUG_LEDS__

		//more to send from this tray
		post LoopControlTask();
	}


	event void AMSend.sendDone(message_t* msg, error_t err)
	{
		atomic pending = FALSE;
		if (err==FAIL)
		{
			dbg("__DBG_CHANNEL__","Sending failed, try resending\n");
			post sendPacketTask();
		}
		else if (err==SUCCESS)
		{
			dbg("__DBG_CHANNEL__","Packet sent successfully\n");
			post sendPacketDoneTask();
		}
	}
}
