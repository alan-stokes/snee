// __MODULE_NAME__
// This module is used to buffer tuples between query plan fragments.

__HEADER__

	#define NUM_SUBTRAYS __NUM_SUBTRAYS__
	#define SUBTRAY_SIZE __SUBTRAY_SIZE__
	#define MAXTUPLESINMSG __MAXTUPLESINMSG__

	__TUPLE_TYPE__ tray[NUM_SUBTRAYS][SUBTRAY_SIZE];
	int8_t subTrayNum=-1;
	int8_t temp;
	int8_t trayHead=-1;
	int8_t trayTail;
	error_t success=SUCCESS;
	int8_t prevSubTrayNum= NUM_SUBTRAYS;
	uint8_t initialized = FALSE;

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
		dbg("__DBG_CHANNEL__","trayGetTuplesDoneTask entered\n");
		dbg("__DBG_CHANNEL__","SubTrayNum: %d, trayHead: %d, trayTail: %d\n",subTrayNum,trayHead,trayTail);
		temp = subTrayNum;
		subTrayNum = -1;
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ subtray set to %d\n",subTrayNum);

		signal GetTuples.requestDataDone(tray[temp], trayHead, trayTail, SUBTRAY_SIZE);
	}

	command error_t GetTuples.open()
	{
		initialize();
		return SUCCESS;
	}	

	command error_t PutTuples.open()
	{
		initialize();
		return SUCCESS;
	}	

	command error_t GetTuples.requestData(nx_int32_t evalEpoch)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  requestData entered\n");

		if (initialized==FALSE)
		{
			initialize();
		}

		trayTail = 0;
		subTrayNum=evalEpoch % NUM_SUBTRAYS;
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  evalEpoch=%d, subTrayNum=%d, pos=%d\n",evalEpoch, subTrayNum,trayTail);
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  evalEpoch at this tray position: %d\n",tray[subTrayNum][trayTail].evalEpoch);

		while ((trayTail<SUBTRAY_SIZE)  && (tray[subTrayNum][trayTail].evalEpoch==evalEpoch))
		{
			atomic
			{
				trayTail++;
				dbg("__DBG_CHANNEL__","trayTail=%d\n",trayTail);
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


	command error_t PutTuples.putTuples(__TUPLE_PTR_TYPE__ inQueue, int8_t inHead, int8_t inTail, uint8_t inQueueSize)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples entered, inHead=%d, inTail=%d, inQueueSize=%d \n",inHead,inTail,inQueueSize);

		if (initialized==FALSE)
		{
			initialize();
		}

		if (inHead > -1)
		{
			dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples entered, evalEpoch of first tuple=%d\n",inQueue[inHead].evalEpoch);

			do
			{
				prevSubTrayNum = subTrayNum;
				subTrayNum = inQueue[inHead].evalEpoch % NUM_SUBTRAYS;
				dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples: subTray=%d, prevSubTray=%d\n",subTrayNum,prevSubTrayNum);

				if (prevSubTrayNum!=subTrayNum)
				{
					dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples: writing to a different subTray Number to the previous one\n");
					trayTail=0;

					while ((trayTail<SUBTRAY_SIZE) &&(tray[subTrayNum][trayTail].evalEpoch==inQueue[inHead].evalEpoch) )
					{
						atomic trayTail++;
						dbg("__DBG_CHANNEL__","__MODULE_NAME__  searching for next available slot, currently looking at: %d\n",trayTail);
					}

					if (trayTail==SUBTRAY_SIZE)
					{
						dbg("__DBG_CHANNEL__","**** Overflow in __MODULE_NAME__ putTuple *****\n");
						trayTail=0;
						success=FAIL;
					}
				}
				dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples writing to subTrayNum %d (of __NUM_SUBTRAYS__) slot %d (of __SUBTRAY_SIZE__) tuple at pos %d with evalEpoch %d\n",subTrayNum,trayTail,inHead,inQueue[inHead].evalEpoch);

__TUPLE_CONSTRUCTION__
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



	command error_t PutTuples.putPacket(__TUPLE_PTR_TYPE__ message)
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__  putPacket entered\n");

		if (initialized==FALSE)
		{
			initialize();
		}

		atomic
		{
			uint8_t inHead=0;

			do
			{
				dbg("__DBG_CHANNEL__","__MODULE_NAME__  inHead=%d\n",inHead);
				if (!(message[inHead].evalEpoch==NULL_EVAL_EPOCH))
				{
					prevSubTrayNum = subTrayNum;
					subTrayNum= message[inHead].evalEpoch % NUM_SUBTRAYS;

					dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples entered, evalEpoch of first tuple=%d\n",message[inHead].evalEpoch);
					dbg("__DBG_CHANNEL__","__MODULE_NAME__  putTuples: subTray=%d, prevSubTray=%d\n",subTrayNum,prevSubTrayNum);

					if (prevSubTrayNum!=subTrayNum)
					{
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
							dbg("__DBG_CHANNEL__","**** Overflow in __MODULE_NAME__ putPacket *****\n");
							trayTail=0;
						}
					}
					dbg("__DBG_CHANNEL__","__MODULE_NAME__  copying subTrayNum=%d, position=%d, tuple num=%d, evalEpoch= %d\n",subTrayNum,trayTail,inHead,message[inHead].evalEpoch);

__TUPLE_CONSTRUCTION2__
					trayTail= trayTail+1;
					if (trayTail == SUBTRAY_SIZE) {
						trayTail = 0;
					}
				}
				else
				{
					dbg("__DBG_CHANNEL__","__MODULE_NAME__   padded tuple encountered\n");
				}
				inHead++;

			} while(inHead < MAXTUPLESINMSG);

			success=SUCCESS;
		}
		return success;
	}
}

