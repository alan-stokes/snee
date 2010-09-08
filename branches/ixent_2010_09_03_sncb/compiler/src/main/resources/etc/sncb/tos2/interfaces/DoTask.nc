
interface DoTask {
	command error_t doTask();
	
	command error_t open();
	
	event void doTaskDone(error_t err);
}


