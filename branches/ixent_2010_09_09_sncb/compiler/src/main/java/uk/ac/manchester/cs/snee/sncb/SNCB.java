package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public interface SNCB {

	public void init();
	
	public void register(SensorNetworkQueryPlan qep) 
	throws SNCBException;
	
	public void deregister();
	
	public void start();
	
	public void stop();
	
	//registerResultListener

}
