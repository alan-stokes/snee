// __OPERATOR_DESCRIPTION__
// This operator defines a time-based window over a stream.

__HEADER__

	#define WINDOW_FROM_IN_EPOCHS __WINDOW_FROM_IN_EPOCHS__
	#define WINDOW_TO_IN_EPOCHS __WINDOW_TO_IN_EPOCHS__
	#define SLIDE __SLIDE_IN_EPOCHS__

	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int inHead;
	int inTail;
	int inQueueSize;

	#define OUT_QUEUE_CARD __OUT_QUEUE_CARD__

	__OUTPUT_TUPLE_TYPE__ windowBuff[OUT_QUEUE_CARD];
	int windowHead=-1;
	int windowTail=-1;

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
	int outHead=-1;
	int outTail=-1;

	int32_t currentEvalEpoch = 0;
	float fromEvalEpoch;
	float toEvalEpoch;

	bool initialized = FALSE;

	void task removeExpiredTuplesTask();
	void task addNewTuplesTask();
	void task copyValidTuplesToOutQueueTask();
	void task signalDoneTask();

	inline void initialize()
	{
		atomic
		{
			int i;
			for (i = 0; i < OUT_QUEUE_CARD; i++)
			{
				windowBuff[i].evalEpoch = NULL_EVAL_EPOCH;
				outQueue[i].evalEpoch = NULL_EVAL_EPOCH;
			}

			initialized = TRUE;
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
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  start values evalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",evalEpoch, windowHead, windowTail, outHead, outTail);

		if (initialized == FALSE) {
		   initialize();
		}

		currentEvalEpoch = evalEpoch;
		fromEvalEpoch = evalEpoch - WINDOW_FROM_IN_EPOCHS;
		toEvalEpoch = evalEpoch - WINDOW_TO_IN_EPOCHS;

		call Child.requestData(evalEpoch);
	   	return SUCCESS;
  	}


	event void Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int _inHead, int _inTail, int _inQueueSize)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled  child\n");

		atomic
		{
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}
		post removeExpiredTuplesTask();
	}


	void task removeExpiredTuplesTask()
	{
		if (windowHead > -1)
		{
			while (windowBuff[windowHead].evalEpoch < fromEvalEpoch)
			{
				windowHead = windowHead + 1;
				if (windowHead == OUT_QUEUE_CARD) 
				{
					windowHead = 0;
			    	}
				if (windowHead==windowTail)
				{
					windowHead=-1;
				}							
			}
		}

		dbg("__DBG_CHANNEL__","__MODULE_NAME__  After removing expired tuples: currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);

		post addNewTuplesTask();
	}

	void task addNewTuplesTask()
	{
		if (inHead > -1)
		{
			do
			{
				if (windowHead == -1)
				{
					windowHead=0;						
					windowTail=0;
				}

__CONSTRUCT_TUPLE__
				windowTail = windowTail+1;
				if (windowTail == OUT_QUEUE_CARD) 
				{
					windowTail = 0;
				}

				inHead= inHead+1;
				if (inHead == inQueueSize) 
				{
					   inHead = 0;
				}
				

			} while (inHead != inTail);
		}
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  After adding new tuples: currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);

		post copyValidTuplesToOutQueueTask();
	}


int tmpWindowHead;

	void task copyValidTuplesToOutQueueTask() 
	{
	     // check if tuples are to be produced for this evalEpoch
	     if (((float)currentEvalEpoch / SLIDE) != (int)((float)currentEvalEpoch / SLIDE))
	     {
		dbg("DBG_USR1","windowOp6Frag3Site1P  No window produced for epoch %d\n", currentEvalEpoch);
		outHead = -1;
		post signalDoneTask();
		return;
	     }

	     if (windowHead == -1)
	     {
		dbg("DBG_USR1","windowOp6Frag3Site1P  Window buffer empty for epoch %d\n", currentEvalEpoch);
		outHead = -1;
		post signalDoneTask();
		return;
	     }

	     outHead = -1;
	     tmpWindowHead = windowHead;

	     do {
	     	if (windowBuff[tmpWindowHead].evalEpoch >= fromEvalEpoch && 
		windowBuff[tmpWindowHead].evalEpoch <= toEvalEpoch)
		{
			if (outHead == -1)
			{
				outHead = 0;
				outTail = 0;
			}			

			dbg("__DBG_CHANNEL__","adding tuple to current window\n");
__CONSTRUCT_TUPLE2__			

			outTail = outTail + 1;
		}
		else
		{
			dbg("DBG_USR1", "Tuple with evalEpoch %d not valid at evalEpoch %d. fromEvalEpoch=%d, toEvalEpoch=%d\n", windowBuff[tmpWindowHead].evalEpoch,currentEvalEpoch);
		}
		tmpWindowHead = tmpWindowHead + 1;
		
		if (tmpWindowHead == OUT_QUEUE_CARD) 
		{
			tmpWindowHead = 0;
		} 
	     } while (tmpWindowHead != windowTail);

	     post signalDoneTask();
	}


	void task signalDoneTask()
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  done with currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);
		signal Parent.requestDataDone(outQueue, outHead, outTail, OUT_QUEUE_CARD);
	}
}
