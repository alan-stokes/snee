package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;

public interface SNCB {

	public void init(String topFile, String resFile) 
	throws SNCBException;
	
	public SNCBSerialPortReceiver register(SensorNetworkQueryPlan qep, 
	String queryOutputDir, MetadataManager metadata) 
	throws SNCBException;
	
	public void deregister(SensorNetworkQueryPlan qep) 
	throws SNCBException;
	
	public void start() throws SNCBException;
	
	public void stop(SensorNetworkQueryPlan qep) 
	throws SNCBException;

}
