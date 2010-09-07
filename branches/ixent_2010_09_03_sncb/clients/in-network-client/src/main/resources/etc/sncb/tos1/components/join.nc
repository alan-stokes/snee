// __OPERATOR_DESCRIPTION__


__HEADER__

	#define OUT_QUEUE_CARD __OUT_QUEUE_CARD__

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
	__LEFT_CHILD_TUPLE_PTR_TYPE__ leftInQueue;
	__RIGHT_CHILD_TUPLE_PTR_TYPE__ rightInQueue;

	int8_t outHead;
	int8_t outTail;
	int16_t currentEvalEpoch;
	int8_t leftInHead;
	int8_t leftInTail;
  	uint16_t leftInQueueSize;
	int8_t rightInHead;
	int8_t rightInTail;
	uint16_t rightInQueueSize;

	command result_t Parent.requestData(int16_t evalEpoch)
  	{
		dbg(DBG_USR2,"__MODULE_NAME__ __OPERATOR_DESCRIPTION__ requestData() entered, now calling left child\n");
		outHead = -1;
		currentEvalEpoch = evalEpoch;
		call LeftChild.requestData(evalEpoch);
	   	return SUCCESS;
  	}

	void task signalDoneTask()
	{
		#ifdef PLATFORM_PC
			int8_t outputTupleCount;
			if (outHead==-1)
			{
				outputTupleCount = 0;
			}
			else if (outHead < outTail)
			{
				outputTupleCount = outTail - outHead;
			}
			else
			{
				outputTupleCount = OUT_QUEUE_CARD - outHead + outTail;
			}
			dbg(DBG_USR2,"JOIN: output cardinality %d tuple(s) for evalEpoch %d\n",outputTupleCount,currentEvalEpoch);
			dbg(DBG_USR3,"outHead=%d, outTail=%d, outQueueCard=%d\n", outHead, outTail, OUT_QUEUE_CARD);
		#endif

		signal Parent.requestDataDone(outQueue, outHead, outTail, OUT_QUEUE_CARD);
	}


	void task performJoin()
	{
		int8_t tmpRightInHead=rightInHead;
		dbg(DBG_USR3,"leftInHead=%d, leftInTail=%d, rightInHead=%d\n", leftInHead, leftInTail, rightInHead);

		do
		{
			tmpRightInHead=rightInHead;
			dbg(DBG_USR3,"rightInHead=%d, rightInTail=%d\n", rightInHead, rightInTail);
			do
			{

				if (__JOIN_PREDICATES__)
				{
					dbg(DBG_USR3,"join predicates met!\n");
						atomic
					{
						if (outHead ==-1)
						{
							outHead=0;
							outTail=0;
						}
						else if (outTail==outHead)
						{
							outHead= outHead+1;
							if (outHead == OUT_QUEUE_CARD) {
							   outHead = 0;
							}
						}
__CONSTRUCT_TUPLE__
						outTail= outTail+1;
						if (outTail == OUT_QUEUE_CARD) {
							outTail = 0;
						}
					}
				}
				else
				{
					dbg(DBG_USR3,"join predicates NOT MET\n");
				}

				tmpRightInHead= tmpRightInHead+1;
				if (tmpRightInHead == rightInQueueSize) {
					tmpRightInHead = 0;
				}
			}
			while(tmpRightInHead!=rightInTail);

			leftInHead= leftInHead+1;
			if (leftInHead == leftInQueueSize) {
				leftInHead = 0;
			}
		}
		while(leftInHead!=leftInTail);

		post signalDoneTask();
	}

	event result_t RightChild.requestDataDone(__RIGHT_CHILD_TUPLE_PTR_TYPE__ _rightInQueue, int8_t _rightInHead, int8_t _rightInTail, uint8_t _rightInQueueSize)
	{
		if (_rightInHead>-1)
		{
			dbg(DBG_USR3,"__MODULE_NAME__ requestDataDone() signalled from right child, start doing join\n");
			rightInQueue=_rightInQueue;
			rightInHead=_rightInHead;
			rightInTail=_rightInTail;
			rightInQueueSize=_rightInQueueSize;

			post performJoin();
		}
		else
		{
			dbg(DBG_USR3,"__MODULE_NAME__ requestDataDone() signalled from right child, No Data\n");
			outHead=-1;
			outTail=-1;
			post signalDoneTask();
		}

		return SUCCESS;
	}


	void task rightRequestData()
	{
		call RightChild.requestData(currentEvalEpoch);
	}


	event result_t LeftChild.requestDataDone(__LEFT_CHILD_TUPLE_PTR_TYPE__ _leftInQueue, int8_t _leftInHead, int8_t _leftInTail, uint8_t _leftInQueueSize)
	{
		if (_leftInHead>-1)
		{
			dbg(DBG_USR3,"__MODULE_NAME__ requestDataDone() signalled from left child, now request data from right child\n");
			atomic
			{
				leftInQueue = _leftInQueue;
				leftInHead = _leftInHead;
				leftInTail = _leftInTail;
				leftInQueueSize = _leftInQueueSize;
			}
			post rightRequestData();
		}
		else
		{
			dbg(DBG_USR3,"__MODULE_NAME__ requestDataDone() signalled from left child, no data\n");
			atomic
			{
				outHead=-1;
				outTail=-1;
			}
			post signalDoneTask();
		}
		return SUCCESS;
	}
}
