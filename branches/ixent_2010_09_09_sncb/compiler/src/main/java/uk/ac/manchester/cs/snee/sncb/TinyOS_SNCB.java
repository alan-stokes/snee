package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class TinyOS_SNCB implements SNCB {

	private static Logger logger = 
		Logger.getLogger(TinyOS_SNCB.class.getName());
	
	TinyOSGenerator codeGenerator;
	
	String nescOutputDir;
	
	String tinyOSEnvVars[];

	private boolean combinedImage = false;

	
	public TinyOS_SNCB(String queryOutputDir, CostParameters costParams)
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			this.nescOutputDir = System.getProperty("user.dir")+"/"+
				queryOutputDir+"tmotesky_t2";
			
			String tosRootDir = SNEEProperties.getSetting(
					SNEEPropertyNames.SNCB_TINYOS_ROOT);
			System.out.println(tosRootDir);
			this.tinyOSEnvVars = new String[] {
					"TOSROOT="+tosRootDir,
					"PATH="+System.getenv("PATH")+":"+tosRootDir+
						"/bin:/opt/local/bin",
					"TOSDIR="+tosRootDir+"/tos",
					"PYTHONPATH=.:"+tosRootDir+"/support/sdk/python",
					"MAKERULES="+tosRootDir+"/support/make/Makerules"};
			this.combinedImage = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
			
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
			codeGenerator = new TinyOSGenerator(tosVersion, tossimFlag, 
				    targetName, combinedImage, queryOutputDir, costParams, controlRadioOff,
				    enablePrintf, useStartUpProtocol, enableLeds,
				    usePowerManagement, deliverLast, adjustRadioPower,
				    includeDeluge, debugLeds, showLocalTime);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}
	

	
	@Override
	public void init() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER init()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN init()");
	}
	

	@Override
	public void register(SensorNetworkQueryPlan qep) 
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER register()");
		try {
			codeGenerator.doNesCGeneration(qep);
			compileNesCCode();
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN register()");
	}
	
	private void compileNesCCode() throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER compileNesCCode()");
		String pythonScript = Utils.getResourcePath("etc/sncb/python/compileNesCCode.py");
		String nescDirParam = "--nesc-dir="+nescOutputDir;
		String params[] = {pythonScript, nescDirParam};
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		if (logger.isTraceEnabled())
			logger.trace("RETURN compileNesCCode()");
	}



	@Override
	public void start() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER start()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN start()");
	}

	@Override
	public void stop() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER stop()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN stop()");
	}

	@Override
	public void deregister() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER deregister()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN deregister()");
	}
}
