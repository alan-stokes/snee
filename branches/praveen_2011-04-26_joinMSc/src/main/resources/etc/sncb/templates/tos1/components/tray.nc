//__MODULE_NAME__
//This module is used to buffer tuples between query plan fragments.

__HEADER__

	#define NUM_SUBTRAYS __NUM_SUBTRAYS__
	#define SUBTRAY_SIZE __SUBTRAY_SIZE__
	#define MAXTUPLESINMSG __MAXTUPLESINMSG__

	__TUPLE_TYPE__ tray[NUM_SUBTRAYS][SUBTRAY_SIZE];
	int8_t subTrayNum=-1;
	int8_t temp;
	int8_t trayHead=-1;
	int8_t trayTail;
	result_t success=SUCCESS;
	int8_t prevSubTrayNum= NUM_SUBTRAYS;
	bool initialized = FALSE;

	inline void initialize()
	{
		atomic
		{
			int i, j;
			for (i = 0; i < NUM_SUBTRAYS; i++)
			{
				for (j = 0; j < SUBTRAY_SIZE; j++)
				{
					tray[i][j].evalEpoch = NULL_EVAL_EPOCH;
				}
			}

			initialized = TRUE;
		}
	}

	void task trayGetTuplesDoneTask()
	{
		__MAX_DEBUG__ dbg(DBG_USR3,"trayGetTuplesDoneTask entered\n");
		__MAX_DEBUG__ dbg(DBG_USR3,"SubTrayNum: %d, trayHead: %d, trayTail: %d\n",subTrayNum,trayHead,trayTail);
		temp = subTrayNum;
		subTrayNum = -1;
		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__ subtray set to %d\n",subTrayNum);
		signal GetTuples.requestDataDone(tray[temp], trayHead, trayTail, SUBTRAY_SIZE);
	}

	command result_t GetTuples.requestData(int16_t evalEpoch)
	{
		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  requestData entered\n");

		if (initialized==FALSE)
		{
			initialize();
		}

		trayTail = 0;
		subTrayNum=evalEpoch % NUM_SUBTRAYS;
		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  evalEpoch=%d, subTrayNum=%d, pos=%d\n",evalEpoch, subTrayNum,trayTail);
		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  evalEpoch at this tray position: %d\n",tray[subTrayNum][trayTail].evalEpoch);
		while ((trayTail<SUBTRAY_SIZE)  && (tray[subTrayNum][trayTail].evalEpoch==evalEpoch))
		{
			atomic
			{
				trayTail++;
				__MAX_DEBUG__ dbg(DBG_USR3,"trayTail=%d\n",trayTail);
			}
		}

		if (trayTail==SUBTRAY_SIZE)
		{
			trayTail = 0; //queue is full
			trayHead=0;
		}
		else if (trayTail==0)
		{
			trayHead=-1;
		}
		else
		{
			trayHead=0;
		}

		post trayGetTuplesDoneTask();

		return SUCCESS;
	}


	command result_t PutTuples.putTuples(__TUPLE_PTR_TYPE__ inQueue, int8_t inHead, int8_t inTail, uint8_t inQueueSize)
	{
		//__GET_TRAY_WRITE_TIMES_EXPREMENT__On();
		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples entered, inHead=%d, inTail=%d, inQueueSize=%d\n",inHead,inTail,inQueueSize);

		if (initialized==FALSE)
		{
			initialize();
		}

		if (inHead > -1)
		{

			__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples entered, evalEpoch of first tuple=%d\n",inQueue[inHead].evalEpoch);

			do
			{
				prevSubTrayNum = subTrayNum;
				subTrayNum = inQueue[inHead].evalEpoch % NUM_SUBTRAYS;
				__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples: subTray=%d, prevSubTray=%d\n",subTrayNum,prevSubTrayNum);

				if (prevSubTrayNum!=subTrayNum)
				{
					__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples: writing to a different subTray Number to the previous one\n");
					trayTail=0;

					while ((trayTail<SUBTRAY_SIZE) &&(tray[subTrayNum][trayTail].evalEpoch==inQueue[inHead].evalEpoch) )
					{
						atomic trayTail++;
						__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  searching for next available slot, currently looking at: %d\n",trayTail);
					}

					if (trayTail==SUBTRAY_SIZE)
					{
						dbg(DBG_USR1,"**** Overflow in __MODULE_NAME__ putTuple *****\n");
						trayTail=0;
						success=FAIL;
					}
				}
				dbg(DBG_USR3,"__MODULE_NAME__  putTuples writing to subTrayNum %d (of __NUM_SUBTRAYS__) slot %d (of __SUBTRAY_SIZE__) tuple at pos %d with evalEpoch %d\n",subTrayNum,trayTail,inHead,inQueue[inHead].evalEpoch);
				//__GET_TRAY_CONSTRUCT_TUPLE1_TIMES_EXPERIMENT__On();

__TUPLE_CONSTRUCTION__
				//__GET_TRAY_CONSTRUCT_TUPLE1_TIMES_EXPERIMENT__Off();
				trayTail= trayTail+1;
				if (trayTail == SUBTRAY_SIZE) {
					trayTail = 0;
				}

				inHead= inHead+1;
				if (inHead ==  inQueueSize) {
					inHead = 0;
				}

			} while(inHead!=inTail);

			success=SUCCESS;
		}
		return success;
	}



	command result_t PutTuples.putPacket(__TUPLE_PTR_TYPE__ message)
	{
		//__TRAY_PUT_PACKET_DEBUG1__On();

		__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putPacket entered\n");

		if (initialized==FALSE)
		{
			initialize();
		}

		atomic
		{

			uint8_t inHead=0;

			do
			{
				__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  inHead=%d\n",inHead);
				if (!(message[inHead].evalEpoch==NULL_EVAL_EPOCH))
				{
					//__TRAY_PUT_PACKET_DEBUG1__Off();

					prevSubTrayNum = subTrayNum;
					subTrayNum= message[inHead].evalEpoch % NUM_SUBTRAYS;

					__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples entered, evalEpoch of first tuple=%d\n",message[inHead].evalEpoch);
					__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__  putTuples: subTray=%d, prevSubTray=%d\n",subTrayNum,prevSubTrayNum);

					if (prevSubTrayNum!=subTrayNum)
					{
						//__TRAY_PUT_PACKET_DEBUG2__On();

						trayTail=0;

						while ((trayTail<SUBTRAY_SIZE) &&(tray[subTrayNum][trayTail].evalEpoch==message[inHead].evalEpoch) )
						{
							atomic
							{
								trayTail++;
							}
						}

						if (trayTail==SUBTRAY_SIZE)
						{
							dbg(DBG_USR1,"**** Overflow in __MODULE_NAME__ putPacket *****\n");
							trayTail=0;
						}
						//__TRAY_PUT_PACKET_DEBUG2__Off();

					}
					dbg(DBG_USR3,"__MODULE_NAME__  copying subTrayNum=%d, position=%d, tuple num=%d, evalEpoch= %d\n",subTrayNum,trayTail,inHead,message[inHead].evalEpoch);
					//__GET_TRAY_CONSTRUCT_TUPLE2_TIMES_EXPERIMENT__On();

__TUPLE_CONSTRUCTION2__
					//__GET_TRAY_CONSTRUCT_TUPLE2_TIMES_EXPERIMENT__Off();

					trayTail= trayTail+1;
					if (trayTail == SUBTRAY_SIZE) {
						trayTail = 0;
					}
				}
				else
				{
					__MAX_DEBUG__ dbg(DBG_USR3,"__MODULE_NAME__   padded tuple encountered\n");
				}
				inHead++;

			} while(inHead < MAXTUPLESINMSG);

			success=SUCCESS;
		}
		return success;
	}

}

