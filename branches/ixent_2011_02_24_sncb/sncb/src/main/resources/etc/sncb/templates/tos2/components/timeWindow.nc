// __OPERATOR_DESCRIPTION__
// This operator defines a time-based window over a stream.

__HEADER__

	#define OUT_QUEUE_CARD __OUT_QUEUE_CARD__
	//Window from and to are expressed as positive number is nesc.
	//This allows for unsigned variables to be used.
	#define WINDOW_FROM __WINDOW_FROM__
	#define WINDOW_TO __WINDOW_TO__
	#define SLIDE __SLIDE__
	#define EVALUATION_INTERVAL __EVALUATION_INTERVAL__

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
	int8_t outHead=-1;
	int8_t outTail=0;
	nx_int32_t currentEvalEpoch;
	bool firstTime = FALSE;
	uint32_t currentEvalTime=0;
	int32_t slideAdjust=0;
	int8_t windowHead=0;
	int8_t windowTail=0;
	int32_t originalEvalTimes[OUT_QUEUE_CARD];
	bool initialized = FALSE;

	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int8_t inHead;
	int8_t inTail;
	uint16_t inQueueSize;

	void task removeExpiredTuplesTask();
	void task outQueueAppendTask();
	void task refreshWindowTask();
	void task signalDoneTask();

	inline void initialize()
	{
		atomic
		{
			int i, j;
			for (i = 0; i < OUT_QUEUE_CARD; i++)
			{
				outQueue[i].evalEpoch = NULL_EVAL_EPOCH;
				originalEvalTimes[i] = NULL_EVAL_EPOCH;
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
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  start values CurrentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);
		
		if (firstTime) {
		   evalEpoch = 0;
		   firstTime = FALSE;
		}

		currentEvalEpoch= evalEpoch;
		currentEvalTime= ((nx_int32_t)evalEpoch) * EVALUATION_INTERVAL;
		call Child.requestData(evalEpoch);
	   	return SUCCESS;
  	}


	event void Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled  child\n");

		atomic
		{
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;

			slideAdjust=currentEvalTime % SLIDE;
		}
		post removeExpiredTuplesTask();
	}


	void task removeExpiredTuplesTask()
	{
		if (currentEvalTime >= slideAdjust + WINDOW_FROM)
		{
			while ((outHead >-1)&& (originalEvalTimes[outHead]<currentEvalTime - WINDOW_FROM -slideAdjust) )
			{
				outHead= outHead+1;
				if (outHead == OUT_QUEUE_CARD) {
				   outHead = 0;
			    }
				if (outHead==outTail)
				{
					outHead=-1;
				}
			}
		}
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  after remove currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d, slideAdjust=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail,slideAdjust);

		post outQueueAppendTask();
	}


	void task outQueueAppendTask()
	{
		if (inHead>-1)
		{
			do
				{
				atomic
				{
					// security Code
					if (outTail==outHead)
					{
						outHead=0 % OUT_QUEUE_CARD;
						dbg("__DBG_CHANNEL__","**** Overflow in __MODULE_NAME__ putTuple *****\n");
					}
				}

				atomic
				{
					if(outHead == -1)
					{
						outHead=0;
						outTail=0;
					}

__CONSTRUCT_TUPLE__

					originalEvalTimes[outTail]=inQueue[inHead].evalEpoch * EVALUATION_INTERVAL;
					outTail= outTail+1;
					if (outTail == OUT_QUEUE_CARD) {
						outTail = 0;
					}
					inHead= inHead+1;
					if (inHead == inQueueSize) {
					   inHead = 0;
				    }
				}

			} while(inHead != inTail);
		}

		dbg("__DBG_CHANNEL__","__MODULE_NAME__  after append currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);
		post refreshWindowTask();
	}


	inline void setEvalTimes()
	{
		int8_t temp=0;
		if (outHead!=-1)
		{
			temp=outHead;
			outQueue[temp].evalEpoch= currentEvalEpoch;
			temp = temp + 1;
			if (temp == OUT_QUEUE_CARD)
			   temp = 0;
			while(temp != outTail)
			{
				outQueue[temp].evalEpoch=currentEvalEpoch;
				temp=(temp+1) % OUT_QUEUE_CARD;
			}
		}
	}


	void task refreshWindowTask() {
		int32_t windowEvalTime=0;
		int8_t indexPos=0;

		windowHead = outHead;
		windowTail= outTail;

		if (currentEvalTime < slideAdjust + WINDOW_TO)
		{
			dbg("__DBG_CHANNEL__","TIME_WINDOW: refreshWindowTask currentEvalTime %d < slideAdjust %d + WINDOW_TO %d\n",currentEvalTime,slideAdjust,WINDOW_TO);
			//All tuples will be held for later
			windowHead=-1;//Empty
			post signalDoneTask();
		}
		else
		{
			// Moves the tail back to ignore the "to" tuples and tuples waiting for the next slide
			windowEvalTime=currentEvalEpoch-slideAdjust + WINDOW_TO;
			if (windowTail == 0) {
			   indexPos = OUT_QUEUE_CARD -1;
		   	} else {
		   	   indexPos = windowTail -1;
	   	   	}
	   	   	//indexPos	=((windowTail-1 + OUT_QUEUE_CARD) % OUT_QUEUE_CARD);
			dbg("__DBG_CHANNEL__","__MODULE_NAME__  comparing indexPos %d with EvalTime %d to %d\n",indexPos,originalEvalTimes[indexPos],windowEvalTime);
			while ((windowHead >-1) && (originalEvalTimes[indexPos]>windowEvalTime) )
			{
				dbg("__DBG_CHANNEL__","__MODULE_NAME__  indexPos %d with EvalTime %d ignored\n",indexPos,originalEvalTimes[indexPos]);
				if (windowTail == 0) {
				   windowTail = OUT_QUEUE_CARD -1;
			   	} else {
			   	   windowTail--;
		   	   	}
			   //windowTail=(windowTail + OUT_QUEUE_CARD -1) % OUT_QUEUE_CARD;
				if (windowHead==windowTail)
				{
					windowHead=-1;//Empty
				}
				if (windowTail == 0) {
				   indexPos = OUT_QUEUE_CARD -1;
			   	} else {
			   	   indexPos = windowTail -1;
		   	   	}
				//indexPos =((windowTail + OUT_QUEUE_CARD -1) % OUT_QUEUE_CARD);
			}
		}

		setEvalTimes();

		post signalDoneTask();
	}


	void task signalDoneTask()
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  done with currentEvalEpoch=%d, windowHead=%d, windowTail=%d, outHead=%d, outTail=%d\n",currentEvalEpoch, windowHead, windowTail, outHead, outTail);
		signal Parent.requestDataDone(outQueue, windowHead, windowTail, OUT_QUEUE_CARD);
	}
}
