
	event TOS_MsgPtr __RECEIVE_INTERFACE__.receive(TOS_MsgPtr msg)
	{

		__TUPLE_MESSAGE_PTR_TYPE__ message = (__TUPLE_MESSAGE_PTR_TYPE__)msg->data;
		dbg(DBG_USR2,"Received __TUPLE_MESSAGE_PTR_TYPE__ from __RECEIVE_INTERFACE__\n");

		call __TRAY_PUT_INTERFACE__.putPacket(message->tuples);

		return msg;
	}