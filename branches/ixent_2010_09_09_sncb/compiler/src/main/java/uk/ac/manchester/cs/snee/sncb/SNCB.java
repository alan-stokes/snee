package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public interface SNCB {

	public void init();
	
	public void register(SensorNetworkQueryPlan qep, String queryOutputDir, CostParameters costParams) 
	throws SNCBException;
	
	public void deregister();
	
	public void start();
	
	public void stop();
	
	//registerResultListener

}
