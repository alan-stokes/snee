// __OPERATOR_DESCRIPTION__
//This operator sends tuples via the serial port.

#include "String.h"

__HEADER__

	#define BUFFERING_FACTOR __BUFFERING_FACTOR__

	int16_t evalEpoch= 0;
	uint8_t bFactorCount=0;
	__CHILD_TUPLE_PTR_TYPE__ inQueue;
	int8_t inHead;
	int8_t inTail;
	uint8_t inQueueSize;

	//used for writing to serial port
	struct TOS_Msg data;
	char deliverStr[500];
	DeliverMessagePtr deliverPacket;
	int sendPos;

	command result_t DoTask.doTask()
  	{
		dbg(DBG_USR2,"__MODULE_NAME__ __OPERATOR_DESCRIPTION__ doTask() entered as evalEpoch %d, now call child\n",evalEpoch);
		dbg(DBG_USR2,"DELIVER do task called\n");
		call Child.requestData(evalEpoch);
	   	return SUCCESS;
  	}

	void task sendToSerialPortTask();

	event result_t SendDeliver.sendDone(TOS_MsgPtr msg, result_t success)
	{
		//__GET_DELIVER_TIMES_EXPERIMENT__Off();
		dbg(DBG_USR2, "sendDone returned\n");
		sendPos += DELIVER_PAYLOAD_SIZE;
		post sendToSerialPortTask();

		return SUCCESS;
	}

	void task deliverDataTask();

	void task sendToSerialPortTask()
	{
		int len;

		deliverPacket = (DeliverMessagePtr)data.data;

		if (sendPos==0)
		{
			dbg(DBG_USR2, deliverStr);
		}

		len = strlen(deliverStr);
		dbg(DBG_USR2, "deliver str length = %d\n", len);

		if (sendPos < len)
		{
			strncpy(deliverPacket->text, deliverStr+sendPos, DELIVER_PAYLOAD_SIZE);

			dbg(DBG_USR2, "About to write to UART... TOS_UART_ADDR=%d, sizeof(DeliverMessage)=%d\n", TOS_UART_ADDR, sizeof(DeliverMessage));
			//__GET_DELIVER_TIMES_EXPERIMENT__On();
			if (call SendDeliver.send(TOS_UART_ADDR, sizeof(DeliverMessage), &data )==FAIL)
			{
				dbg(DBG_USR1, "FAILURE writing to node %d serial port\n", TOS_LOCAL_ADDRESS);
			}
			else
			{
				dbg(DBG_USR2, "SUCCESS writing to node %d serial port\n", TOS_LOCAL_ADDRESS);
			}

			dbg(DBG_USR2, "PACKET %s \n", deliverPacket->text);
		}
		else
		{
			post deliverDataTask();
		}
	}

	void task deliverDataTask()
	{
		deliverStr[0] = '\0';
		if (inHead >-1)
		{
			char tmpStr[30];
			strcat(deliverStr, "DELIVER: (");
__CONSTRUCT_DELIVER_TUPLE_STR__
			strcat(deliverStr, ")\n");
			inHead= inHead+1;
			if (inHead == inQueueSize) {
			  inHead = 0;
		    }
		    if (inHead == inTail) {
		    	inHead = -1;
	    	} 

			dbg(DBG_USR1, "%s", deliverStr);
			sendPos = 0;
			post sendToSerialPortTask();
		}
		else
		{
			bFactorCount++;
			if (bFactorCount< BUFFERING_FACTOR)
			{
				evalEpoch++;
				call Child.requestData(evalEpoch);
			}
			else
			{
				evalEpoch++;
				bFactorCount = 0;
			}
		}
	}

	event result_t Child.requestDataDone(__CHILD_TUPLE_PTR_TYPE__ _inQueue, int8_t _inHead, int8_t _inTail, uint8_t _inQueueSize)
	{
		dbg(DBG_USR2,"__MODULE_NAME__ requestDataDone() signalled from child, delivering data\n");

		atomic
		{
			inQueue = _inQueue;
			inHead = _inHead;
			inTail = _inTail;
			inQueueSize = _inQueueSize;
		}

		post deliverDataTask();

		return SUCCESS;
	}



}
