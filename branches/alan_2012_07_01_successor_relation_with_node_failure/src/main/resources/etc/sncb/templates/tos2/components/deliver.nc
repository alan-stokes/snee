// __OPERATOR_DESCRIPTION__
// This operator sends tuples via the serial port.

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	nx_int32_t evalEpoch;
	bool firstTime = TRUE;
	int bFactorCount=0;
	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int inHead;
	int inTail;
	int inQueueSize;

	message_t packet;
	DeliverMessagePtr payload;

	error_t result;
	int tuplePacketPos;
	uint8_t pending=FALSE;

	void task sendPacketTask();
	void task signalTaskDoneTask();
	void task loopControlTask();
	void task sendPacketTask();
	void task sendPacketDoneTask();
	void task signalTaskDoneTask();

	command error_t DoTask.open()
	{
		call Child.open();
		return SUCCESS;
	}

	command error_t DoTask.doTask()
  	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ doTask() entered as evalEpoch %d, now call child\n",evalEpoch);

		bFactorCount = 0;
		tuplePacketPos = 0;

		if (firstTime) 
		{
			evalEpoch = 0;
		   	firstTime = FALSE;
		}
		call Child.requestData(evalEpoch);

	   	return SUCCESS;
  	}

	event void Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int _inHead, int _inTail, int _inQueueSize)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from child, delivering data\n" );

		atomic
		{
			bFactorCount++;
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}

		post loopControlTask();
	}

	void task loopControlTask()
	{
		dbg("__DBG_CHANNEL__", "loopControlTask: more tuples in the inQueue\n");

		payload = (DeliverMessagePtr)(call Packet.getPayload(&packet, sizeof(DeliverMessage)));
		if (call Packet.maxPayloadLength() < sizeof(DeliverMessage) || payload == NULL)
		{
			dbg("DBG_USR1","Message type DeliverMessage size=%d, too big for packet payload=%d.\n",sizeof(DeliverMessage),payload);
			return;
      		}

		if (inHead >-1)
		{
		    	dbg("DBG_USR1", __CONSTRUCT_DELIVER_TUPLE__);

			//generate tuples
			dbg("__DBG_CHANNEL__", "loopControlTask: Generating tuple at pos %d in inQueue\n",inHead);
			payload->tuples[tuplePacketPos] = inQueue[inHead];
			inHead = inHead +1;
			if (inHead == inQueueSize) 
			{
				inHead = 0;
		        }
			if (inHead == inTail)
			{
				inHead = -1;
			}
			tuplePacketPos++;
		}

		if ((tuplePacketPos == __TUPLES_PER_PACKET__)) //current packet is full
		{
			dbg("__DBG_CHANNEL__", "loopControlTask: Current packet is full\n");
			post sendPacketTask();
		}
		else if (inHead >-1) // more tuples in inQueue
		{
			dbg("__DBG_CHANNEL__", "loopControlTask: More tuples in queue\n");
			post loopControlTask();
		}
		else if (bFactorCount < BUFFERING_FACTOR) // more data still buffered at other evaluation times
		{
			dbg("__DBG_CHANNEL__", "loopControlTask: More data still buffered at other evaluation times\n");
			evalEpoch++;
			call Child.requestData(evalEpoch);
		}

		else // no more data at other evaluation times
		{
			if (tuplePacketPos>0)  
			{
				dbg("__DBG_CHANNEL__", "loopControlTask: Buffering factor reached, send non-full packet.\n");
				post sendPacketTask();
			}
			else
			{
				dbg("__DBG_CHANNEL__", "loopControlTask: No more tuples to send in this agend evaluation.\n");				evalEpoch++;

				post signalTaskDoneTask();
			}
		}
	}

	void task sendPacketTask()
	{
		if (pending)
		{
			dbg("__DBG_CHANNEL__", "Still pending...\n");
			post sendPacketTask();
		}
		else
		{
			atomic pending = TRUE;
			dbg("__DBG_CHANNEL__","Attempting to send DeliverMessage packet to serial port...\n");

			//Pad any unsed tuples
			while (tuplePacketPos < __TUPLES_PER_PACKET__) // tuples per packet for source fragment type
			{
				payload->tuples[tuplePacketPos].evalEpoch = NULL_EVAL_EPOCH;
				tuplePacketPos++;
			}

			result = call SendDeliver.send(__PARENT_ID__,&packet, sizeof(DeliverMessage)); 
			if (result==SUCCESS)
			{
				dbg("__DBG_CHANNEL__", "Message accepted by active message layer in __MODULE_NAME__.\n");
			}
			else
			{
				dbg("__DBG_CHANNEL__","Error: Call to active message layer failed in __MODULE_NAME__ with error code %d  (%d=FAIL, %d=EBUSY, %d=ECANCEL).\n",result,FAIL,EBUSY,ECANCEL);
				atomic pending = FALSE;
				//post sendPacketTask(); // retry sending packet (comment out accordingly)
				post sendPacketDoneTask(); // do not retry sending packet (comment out accordingly)

			}
		}

	}		

	event void SendDeliver.sendDone(message_t* msg, error_t err)
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

	void task sendPacketDoneTask()
	{
		atomic pending = FALSE;
		tuplePacketPos=0;

		//more to send from this tray
		post loopControlTask();

	}

	void task signalTaskDoneTask()
	{
		signal DoTask.doTaskDone(SUCCESS);
	}

}
