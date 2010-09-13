package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public interface SNCB {

	public void init(String topFile, String resFile) throws SNCBException;
	
	public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep, 
	String queryOutputDir, CostParameters costParams) throws SNCBException;
	
	public void deregister();
	
	public void start();
	
	public void stop();
	
	//registerResultListener

}
