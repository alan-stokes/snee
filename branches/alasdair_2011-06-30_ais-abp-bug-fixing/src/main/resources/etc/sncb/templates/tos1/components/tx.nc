//__MODULE_DESCRIPTION__
//This module reads tuples from one or more trays and sends them over the radio.

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	//vars shared for all trays (since we only process one at a time)
	int8_t bFactorCount;
	int16_t evalEpoch =  0;
	int16_t currentEvalEpoch =  0;
  	struct TOS_Msg data;
	int8_t inHead;
	int8_t inTail;
	int8_t inQueueSize;

	int8_t tuplePacketPos;
	bool pending=FALSE;

__DECLS__



	command result_t DoTask.doTask()
  	{
  	
		dbg(DBG_USR2,"__MODULE_NAME__ doTask() entered as evalEpoch %d, now get tuples from trays and send them on\n",evalEpoch);
		dbg(DBG_USR2,"RADIO start from __FROM_SITE__ to __TO_SITE__\n");

		bFactorCount = 0;
		tuplePacketPos = 0;

		__FIRST_MESSAGE_PTR_TYPE_INSTANCE__ = (__FIRST_MESSAGE_PTR_TYPE__)data.data;

		//get the first tray
		call __FIRST_TRAY_CALL__.requestData(evalEpoch);
		return SUCCESS;
	}

__TX_METHODS__

}
