includes QueryPlan;


interface __INTERFACE_NAME__
{
	command result_t requestData(int16_t evalEpoch);

	event result_t requestDataDone(__TUPLE_TYPE_PTR__ inQueue, int8_t head, int8_t tail, uint8_t inQueueSize);
}


