// __OPERATOR_DESCRIPTION__
//This module represents the producer part of the exchange operator, which when
//triggered receives tuples from its child operator and places them in a tray.

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	int16_t evalEpoch= 0;
	uint8_t bFactorCount=0;
	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int8_t inHead;
	int8_t inTail;
	uint16_t inQueueSize;

	command result_t DoTask.doTask()
	{
		dbg(DBG_USR2,"__MODULE_NAME__ __OPERATOR_DESCRIPTION__ doTask() entered as evalEpoch %d, now call child\n",evalEpoch);
		call Child.requestData(evalEpoch);
		return SUCCESS;
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
			dbg(DBG_USR2," producer moving to next buffering loop %d \n",evalEpoch);
			call Child.requestData(evalEpoch);
		}
		else
		{
			evalEpoch++;
			dbg(DBG_USR2,"next evalEpoch %d \n\n",evalEpoch);
			bFactorCount=0;
		}

	}

	event result_t Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg(DBG_USR2,"__MODULE_NAME__ requestDataDone() signalled from child, put results in tray\n");
		atomic
		{
			inQueue=_inQueue;
			inHead=_inHead;
			inTail=_inTail;
			inQueueSize=_inQueueSize;
		}
		post trayPutTask();

		return SUCCESS;
	}

}
