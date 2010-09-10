package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public class TinyOS_SNCB implements SNCB {

	private static Logger logger = 
		Logger.getLogger(TinyOS_SNCB.class.getName());
	
	private SensorNetworkSourceMetadata metadata;
	
	private String tinyOSEnvVars[];

	private boolean combinedImage = false;

	
	public TinyOS_SNCB(SensorNetworkSourceMetadata metadata)
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			this.metadata = metadata;
			String tosRootDir = SNEEProperties.getSetting(
					SNEEPropertyNames.SNCB_TINYOS_ROOT);
			System.out.println(tosRootDir);
			//TinyOS environment variables
			this.tinyOSEnvVars = new String[] {
					"TOSROOT="+tosRootDir,
					"PATH="+System.getenv("PATH")+":"+tosRootDir+
						"/bin:/opt/local/bin:/Library/Java/Home/bin",
					"TOSDIR="+tosRootDir+"/tos",
					"PYTHONPATH=.:"+tosRootDir+"/support/sdk/python",
					"MAKERULES="+tosRootDir+"/support/make/Makerules"};
			this.combinedImage = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}
	

	
	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER init()");
		try {
			//TODO: incorporate metadata collection python script
			String pythonScript = Utils.getResourcePath("etc/sncb/python/collectMetadata.py");
			String params[] = {pythonScript, topFile, resFile};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN init()");
	}
	

	@Override
	public void register(SensorNetworkQueryPlan qep, String queryOutputDir, CostParameters costParams) 
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER register()");
		try {			
			logger.trace("Generating TinyOS/nesC code for query plan.");
			generateNesCCode(qep, queryOutputDir, costParams);
			logger.trace("Compiling TinyOS/nesC code into executable images.");
			compileNesCCode(queryOutputDir);
			logger.trace("Disseminating Query Plan images");
			////disseminateQueryPlanImages();
			logger.trace("Setting up result collector");
			////setUpResultCollector(qep, queryOutputDir);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN register()");
	}

	private void generateNesCCode(SensorNetworkQueryPlan qep, String 
	queryOutputDir, CostParameters costParams) throws IOException, 
	SchemaMetadataException, TypeMappingException, OptimizationException,
	CodeGenerationException {
		//TODO: move some of these to an sncb .properties file
		int tosVersion = 2;
		boolean tossimFlag = false;
		String targetName = "tmotesky_t2";
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
		TinyOSGenerator codeGenerator = new TinyOSGenerator(tosVersion, tossimFlag, 
			    targetName, combinedImage, queryOutputDir, costParams, controlRadioOff,
			    enablePrintf, useStartUpProtocol, enableLeds,
			    usePowerManagement, deliverLast, adjustRadioPower,
			    includeDeluge, debugLeds, showLocalTime);
		//TODO: in the code generator, need to connect controller components to query plan components
		codeGenerator.doNesCGeneration(qep);		
	}
	
	private void compileNesCCode(String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER compileNesCCode()");
		String nescOutputDir = System.getProperty("user.dir")+"/"+
		queryOutputDir+"tmotesky_t2";
		String pythonScript = Utils.getResourcePath("etc/sncb/python/compileNesCCode.py");
		String nescDirParam = "--nesc-dir="+nescOutputDir;
		String params[] = {pythonScript, nescDirParam};
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		if (logger.isTraceEnabled())
			logger.trace("RETURN compileNesCCode()");
	}

	
	private void setUpResultCollector(SensorNetworkQueryPlan qep, 
	String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setUpResultCollector()");
		//TODO: need to set up plumbing for query result collection (using mig?)
		String nescOutputDir = System.getProperty("user.dir")+"/"+
		queryOutputDir+"tmotesky_t2";
		String nesCHeaderFile = nescOutputDir+"/mote"+qep.getGateway()+"/QueryPlan.h";
		System.out.println(nesCHeaderFile);
		String outputJavaFile = System.getProperty("user.dir")+"/"+queryOutputDir+"DeliverMessage.java";
		String params[] = {"java", "-target=null", "-java-classname=DeliverMessage",
				nesCHeaderFile, "DeliverMessage", "-o "+outputJavaFile};
		Utils.runExternalProgram("mig", params, this.tinyOSEnvVars);
		if (logger.isTraceEnabled())
			logger.trace("RETURN setUpResultCollector()");	
	}
	
	private void disseminateQueryPlanImages() {
		//TODO: invoke OTA functionality to disseminate query plan
	}
	
	@Override
	public void start() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER start()");
		//TODO: invoke python script sending start command to initiate query execution
		//TODO: evaluator needs to receive ect results and send them to collector
		if (logger.isDebugEnabled())
			logger.debug("RETURN start()");
	}

	@Override
	public void stop() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER stop()");
		//TODO: invoke python script sending stop command to terminate query execution
		if (logger.isDebugEnabled())
			logger.debug("RETURN stop()");
	}

	@Override
	public void deregister() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER deregister()");
		//TODO: invoke python script for OTA to remove current query image 
		if (logger.isDebugEnabled())
			logger.debug("RETURN deregister()");
	}
}
