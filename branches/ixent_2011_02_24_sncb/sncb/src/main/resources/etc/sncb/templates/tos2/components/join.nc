// __OPERATOR_DESCRIPTION__


__HEADER__

	#define OUT_QUEUE_CARD __OUT_QUEUE_CARD__

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
	__LEFT_CHILD_TUPLE_PTR_TYPE__ leftInQueue;
	__RIGHT_CHILD_TUPLE_PTR_TYPE__ rightInQueue;

	int8_t outHead;
	int8_t outTail;
	nx_int32_t currentEvalEpoch;
	int8_t leftInHead;
	int8_t leftInTail;
  	uint16_t leftInQueueSize;
	int8_t rightInHead;
	int8_t rightInTail;
	uint16_t rightInQueueSize;

	void task rightRequestDataTask();
	void task performJoinTask();
	void task signalDoneTask();

	command error_t Parent.open()
	{
		call LeftChild.open();
		call RightChild.open();
		return SUCCESS;
	}	

	command error_t Parent.requestData(nx_int32_t evalEpoch)
  	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ requestData() entered, now calling left child\n");
		outHead = -1;
		currentEvalEpoch = evalEpoch;
		call LeftChild.requestData(evalEpoch);
	   	return SUCCESS;
  	}

	event void LeftChild.requestDataDone(__LEFT_CHILD_TUPLE_PTR_TYPE__ _leftInQueue, int8_t _leftInHead, int8_t _leftInTail, uint8_t _leftInQueueSize)
	{
		if (_leftInHead>-1)
		{
			dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from left child, now request data from right child\n");
			atomic
			{
				leftInQueue = _leftInQueue;
				leftInHead = _leftInHead;
				leftInTail = _leftInTail;
				leftInQueueSize = _leftInQueueSize;
			}
			post rightRequestDataTask();
		}
		else
		{
			dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from left child, no data\n");
			atomic
			{
				outHead=-1;
				outTail=-1;
			}
			post signalDoneTask();
		}	
	}


	void task rightRequestDataTask()
	{
		call RightChild.requestData(currentEvalEpoch);
	}


	event void RightChild.requestDataDone(__RIGHT_CHILD_TUPLE_PTR_TYPE__ _rightInQueue, int8_t _rightInHead, int8_t _rightInTail, uint8_t _rightInQueueSize)
	{
		if (_rightInHead>-1)
		{
			dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from right child, start doing join\n");
			rightInQueue=_rightInQueue;
			rightInHead=_rightInHead;
			rightInTail=_rightInTail;
			rightInQueueSize=_rightInQueueSize;

			post performJoinTask();
		}
		else
		{
			dbg("__DBG_CHANNEL__","__MODULE_NAME__ requestDataDone() signalled from right child, No Data\n");
			outHead=-1;
			outTail=-1;
			dbg("DBG_USR1", "Performing join\n");
			post signalDoneTask();
		}
	
	}

	void task performJoinTask()
	{
		int8_t tmpRightInHead=rightInHead;
		dbg("__DBG_CHANNEL__","leftInHead=%d, leftInTail=%d, rightInHead=%d\n", leftInHead, leftInTail, rightInHead);
		
		do
		{
			tmpRightInHead=rightInHead;
			dbg("__DBG_CHANNEL__","rightInHead=%d, rightInTail=%d\n", rightInHead, rightInTail);
			do 
			{

				if (__JOIN_PREDICATES__)
				{
					dbg("__DBG_CHANNEL__","join predicates met!\n");
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
					dbg("__DBG_CHANNEL__","join predicates NOT MET\n");
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

	void task signalDoneTask()
	{
		dbg("__DBG_CHANNEL__","outHead=%d, outTail=%d, outQueueCard=%d\n", outHead, outTail, OUT_QUEUE_CARD);
		signal Parent.requestDataDone(outQueue, outHead, outTail, OUT_QUEUE_CARD);
	}
}
