// __OPERATOR_DESCRIPTION__
// This operator performs part of an incremental aggregation.

__HEADER__

	#define OUT_QUEUE_CARD __OUT_QUEUE_CARD__

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
	int8_t outHead;
	int8_t outTail;
	int16_t currentEvalEpoch;

  	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int8_t inHead;
	int8_t inTail;
	int8_t inHead2;
	int8_t inTail2;
	uint8_t inQueueSize;
__AGGREGATE_VAR_DECLS__


	void task aggregateIncrementTask();
	void task computeFinalAggregatesTask();
	void task signalDoneTask();
	void task outQueueAppendTask();

	inline void initialize()
	{
		atomic
		{
			outHead=-1;
			outTail=0;

__AGGREGATE_VAR_INITIALIZATION__
			currentEvalEpoch=0;
		}
	}

	command error_t Parent.open()
	{
		call Child.open();
		return SUCCESS;
	}	


	command error_t Parent.requestData(nx_int32_t evalEpoch)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ requestData() entered, now calling child\n");
		initialize();
		currentEvalEpoch = evalEpoch;

		call Child.requestData(evalEpoch);

		return SUCCESS;
	}

	event void Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		atomic
		{
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}

		post aggregateIncrementTask();
	}

	void task aggregateIncrementTask()
	{
		inHead2 = inHead;
		inTail2 = inTail;
		if (inHead>-1)
		{
			do
			{
__AGGREGATE_VAR_INCREMENT__
				inHead=(inHead+1)%inQueueSize;
			}
			while(inHead!=inTail);
			post computeFinalAggregatesTask();
		}
		else
		{
			outHead=-1;
			outTail=-1;
			post signalDoneTask();
		}
	}

__DERIVED_INCREMENTAL_AGGREGATES_DECLS__

	void task computeFinalAggregatesTask()
	{

__COMPUTE_DERIVED_INCREMENTAL_AGGREGATES__
		
		post outQueueAppendTask();
	}


	void task outQueueAppendTask()
	{
		if (outHead==outTail)
		{
			outHead= outHead+1;
			if (outHead == OUT_QUEUE_CARD) 
			{
			   outHead = 0;
			}
		}
		if (outHead ==-1)
		{
			outHead=0;
			outTail=0;
		}

__CONSTRUCT_TUPLE__

		outTail= outTail+1;
		if (outTail == OUT_QUEUE_CARD) 
		{
			outTail = 0;
        	}
		dbg("DBG_USR1", "Performing aggregation\n");
		post signalDoneTask();
	}



	void task signalDoneTask()
	{
		dbg("__DBG_CHANNEL__","outHead=%d, outTail=%d, OUT_QUEUE_CARD=%d\n",outHead, outTail, OUT_QUEUE_CARD);
		signal Parent.requestDataDone(outQueue, outHead, outTail, OUT_QUEUE_CARD);
	}


}
