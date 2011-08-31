package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;

import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

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

  public void waitForQueryEnd() throws InterruptedException;
  
  
  public void setOutputFolder(String newTargetDir);
  
  public void generateNesCCode(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager metadata)
  throws IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException;

  public void compileNesCCode(String queryOutputDir) throws IOException;
}
