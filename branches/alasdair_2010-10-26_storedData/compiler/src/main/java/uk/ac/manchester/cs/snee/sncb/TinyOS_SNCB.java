package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public class TinyOS_SNCB implements SNCB {

	TinyOSGenerator codeGenerator;
	
	public TinyOS_SNCB(String nescOutputDir, CostParameters costParams)
	throws IOException, SchemaMetadataException, TypeMappingException {
		//TODO: parse this from an sncb .properties file
		int tosVersion = 2;
		boolean tossimFlag = false;
		String targetName = "tmotesky_t2";
		boolean combinedImage= false;
		boolean controlRadioOff = false;
		boolean enablePrintf = false;
		boolean useStartUpProtocol = true;
		boolean enableLeds = true;
		boolean usePowerManagement = false;
		boolean deliverLast = false;
		boolean adjustRadioPower = false;
		boolean includeDeluge = false;
		boolean debugLeds = true;
		boolean showLocalTime = false;
		codeGenerator = new TinyOSGenerator(tosVersion, tossimFlag, 
			    targetName, combinedImage, nescOutputDir, costParams, controlRadioOff,
			    enablePrintf, useStartUpProtocol, enableLeds,
			    usePowerManagement, deliverLast, adjustRadioPower,
			    includeDeluge, debugLeds, showLocalTime);		
	}
	

	
	@Override
	public void init() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void deregister() {
		// TODO Auto-generated method stub

	}

	@Override
	public void register(SensorNetworkQueryPlan qep) 
	throws OptimizationException, CodeGenerationException, SchemaMetadataException, 
	TypeMappingException {
		codeGenerator.doNesCGeneration(qep);
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
