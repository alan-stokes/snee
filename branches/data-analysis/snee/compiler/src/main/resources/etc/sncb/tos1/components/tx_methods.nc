
	void task __TRAY_PREFIX__SendPacketDoneTask();

	void task __TRAY_PREFIX__SendPacketTask()
	{
		if (pending)
		{
			post __TRAY_PREFIX__SendPacketTask();
		}
		else
		{
			atomic pending = TRUE;
			//printTime(timeBuf, 128);
			//dbg(DBG_USR2,"At %s Attempting to send __TUPLE_MESSAGE_TYPE__ packet to __PARENT_ID__\n",timeBuf);

			//Pad any unsed tuples
			while (tuplePacketPos < __TUPLES_PER_PACKET__) // tuples per packet for source fragment type
			{
				__TRAY_PREFIX__Packet->tuples[tuplePacketPos].evalEpoch = NULL_EVAL_EPOCH;
				tuplePacketPos++;
			}
			dbg(DBG_USR2,"call to __SEND_INTERFACE__.send(__PARENT_ID__, size = %d) started\n",sizeof(__TUPLE_MESSAGE_TYPE__));

			//__GET_RADIO_TIMES_EXPERIMENT__On();
			if (call __SEND_INTERFACE__.send(__PARENT_ID__, sizeof(__TUPLE_MESSAGE_TYPE__), &data )==FAIL)
			{
				dbg(DBG_USR2,"Error: call to __SEND_INTERFACE__.send(__PARENT_ID__, size = %d) FAILED\n",sizeof(__TUPLE_MESSAGE_TYPE__));
				atomic pending = FALSE;
				//post __TRAY_PREFIX__SendPacketTask(); // retry sending packet (comment out accordingly)
				post __TRAY_PREFIX__SendPacketDoneTask(); // do not retry sending packet (comment out accordingly)

			}
		}
	}

	void task __TRAY_PREFIX__LoopControlTask()
	{
		//more tuples in the inQueue
		if (inHead >-1)
		{
			//generate tuples
			__TRAY_PREFIX__Packet->tuples[tuplePacketPos] = __TRAY_PREFIX__InQueue[inHead];
			inHead = inHead+1;
			if (inHead == inQueueSize) {
				inHead = 0;
			}
			if (inHead==inTail)
			{
				inHead = -1;
			}
			tuplePacketPos++;
		}

		if ((tuplePacketPos == __TUPLES_PER_PACKET__)) //current packet is full
		{
			post __TRAY_PREFIX__SendPacketTask();
		}
		else if (inHead >-1) // more tuples in inQueue
		{
			post __TRAY_PREFIX__LoopControlTask();
		}
		else if (bFactorCount < BUFFERING_FACTOR) // more data still buffered at other evaluation epochs
		{
			evalEpoch++;
			call __TRAY_CALL__.requestData(evalEpoch);
		}
		else // no more data at other evaluation epochs
		{

			if (tuplePacketPos>0) // packet being constructed contains tuples, send
			{
				post __TRAY_PREFIX__SendPacketTask();
			}
			else
			{
				evalEpoch++;
__NEXT_TRAY_CALL__
			}

		}
	}

	void task __TRAY_PREFIX__SendPacketDoneTask()
	{
		//__GET_RADIO_TIMES_EXPERIMENT__Off();
#ifdef PLATFORM_PC  //Ixent added this for SIGMOD08 experiment 4
			dbg(DBG_POWER,"*** Sent QUERY RESULT message at time %lld\n ***",tos_state.tos_time);
#endif
		atomic pending = FALSE;
		tuplePacketPos=0;

		//more to send from this tray
		post __TRAY_PREFIX__LoopControlTask();

	}

	event result_t __SEND_INTERFACE__.sendDone(TOS_MsgPtr msg, result_t success)
	{
		atomic pending = FALSE;
		if (success==FAIL)
		{
			dbg(DBG_USR2,"Sending failed, try resending\n");
			post __TRAY_PREFIX__SendPacketTask();
		}
		else
		{
			dbg(DBG_USR2,"Packet sent successfully\n");
			post __TRAY_PREFIX__SendPacketDoneTask();
		}

		return SUCCESS;
	}


	event result_t __TRAY_CALL__.requestDataDone(__TRAY_PREFIX_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg(DBG_USR2, "tx:__TRAY_CALL__.requestDataDone ente\n");

		atomic
		{
			bFactorCount++;
			__TRAY_PREFIX__InQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}

		post __TRAY_PREFIX__LoopControlTask();

		return SUCCESS;
	}