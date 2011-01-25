package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public interface SNCB {

	public void init(String topFile, String resFile) 
	throws SNCBException;
	
	public SNCBSerialPortReceiver register(SensorNetworkQueryPlan qep, 
	String queryOutputDir, CostParameters costParams) 
	throws SNCBException;
	
	public void deregister(SensorNetworkQueryPlan qep) 
	throws SNCBException;
	
	public void start() throws SNCBException;
	
	public void stop(SensorNetworkQueryPlan qep) 
	throws SNCBException;

}
