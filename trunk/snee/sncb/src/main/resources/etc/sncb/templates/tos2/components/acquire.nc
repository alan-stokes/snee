// __OPERATOR_DESCRIPTION__
// This operator takes one or more sensor readings for a streaming logical extent.

__HEADER__

	#define OUT_QUEUE_CARD 1

	__OUTPUT_TUPLE_TYPE__ outQueue[OUT_QUEUE_CARD];
__READING_VAR_DECLS__
  	int8_t outHead=-1;
	int8_t outTail=0;
	nx_int32_t currentEvalEpoch;

	command error_t Parent.requestData(nx_int32_t evalEpoch)
  	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ requestData() entered, now acquire data\n");
		atomic currentEvalEpoch = evalEpoch;
		atomic
		{
			acquiring0 = TRUE;
		}

		call Read0.read();
	   	return SUCCESS;
  	}

	command error_t Parent.open()
	{
		acquiring0 = FALSE;
		//Return from this ADC call will be ignored; 
		call Read0.read();
		//calls to other ADC to be added as required.
		return SUCCESS;
	}	


	void task signalDoneTask()
	{
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ requestDataDone() entered\n");
		signal Parent.requestDataDone(outQueue, outHead, outTail, OUT_QUEUE_CARD);
	}



	void task constructTupleTask()
	{
		__NESC_DEBUG_LEDS__
		dbg("__DBG_CHANNEL__","__MODULE_NAME__ __OPERATOR_DESCRIPTION__ constructTupleTask entered\n");
		atomic
		{
			if (__FULL_ACQUIRE_PREDICATES__)
			{
				outHead=0;
				outTail=0;
__CONSTRUCT_TUPLE__
				dbg("DBG_USR1", __CONSTRUCT_TUPLE_STR__);
			}
			else
			{
				outHead=-1;
				dbg("DBG_USR1","Tuple discarded to predicate __ACQUIRE_PREDICATES__\n");
			}

		}

		post signalDoneTask();
	}

__GET_DATA_METHODS__

}
