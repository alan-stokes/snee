


interface __PUT_INTERFACE_NAME__
{
	command error_t putTuples(__TUPLE_TYPE_PTR__ inQueue, int8_t inHead, int8_t inTail, uint8_t inQueueSize);
	command error_t putPacket(__TUPLE_TYPE_PTR__ message);
	command error_t open();
	
}


