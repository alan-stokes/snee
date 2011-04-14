


interface __PUT_INTERFACE_NAME__
{
	command error_t putTuples(__TUPLE_TYPE_PTR__ inQueue, int inHead, int inTail, int inQueueSize);
	command error_t putPacket(__TUPLE_TYPE_PTR__ message);
	command error_t open();
	
}


