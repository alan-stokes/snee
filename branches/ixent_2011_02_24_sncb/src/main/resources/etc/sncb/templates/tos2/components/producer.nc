// __OPERATOR_DESCRIPTION__
//This module represents the producer part of the exchange operator, which when
//triggered receives tuples from its child operator and places them in a tray.

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	nx_int32_t evalEpoch;
	bool firstTime = TRUE;
	uint8_t bFactorCount=0;
	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int8_t inHead;
	int8_t inTail;
	uint16_t inQueueSize;

	void task trayPutTask();

	command error_t DoTask.open()
	{
		call Child.open();
		call PutTuples.open();
		return SUCCESS;
	}	

	command error_t DoTask.doTask()
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ doTask() entered as evalEpoch %d, now call child\n",evalEpoch);

		if (firstTime) {
		   evalEpoch = 0;
		   firstTime = FALSE;
		}

		call Child.requestData(evalEpoch);
		return SUCCESS;
	}

	event void Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from child, put results in tray\n");
		atomic
		{
			inQueue=_inQueue;
			inHead=_inHead;
			inTail=_inTail;
			inQueueSize=_inQueueSize;
		}
		post trayPutTask();

	}

	void task trayPutTask()
	{
		if (inHead > -1)
		{
			call PutTuples.putTuples(inQueue, inHead, inTail, inQueueSize);
		}


		bFactorCount++;
		if (bFactorCount < BUFFERING_FACTOR)
		{
			evalEpoch++;
			dbg("__DBG_CHANNEL__"," producer moving to next buffering loop %d \n",evalEpoch);
			call Child.requestData(evalEpoch);
		}
		else
		{
			evalEpoch++;
			dbg("__DBG_CHANNEL__","next evalEpoch %d \n\n",evalEpoch);
			bFactorCount=0;
			signal DoTask.doTaskDone(SUCCESS);
		}

	}


}
