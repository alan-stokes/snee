package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;

import net.tinyos.message.Message;
import net.tinyos.tools.MsgReader;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public class TinyOS_SNCB implements SNCB {

	private Logger logger = 
		Logger.getLogger(TinyOS_SNCB.class.getName());
	
	private SensorNetworkSourceMetadata metadata;
	
	private String tinyOSEnvVars[];

	private boolean combinedImage = false;

	private String serialPort = "/dev/tty.usbserial-XBTE03FA";
	
	private boolean demoMode = true;
	
	public TinyOS_SNCB(SensorNetworkSourceMetadata metadata)
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			this.metadata = metadata;
			String tosRootDir = SNEEProperties.getSetting(
					SNEEPropertyNames.SNCB_TINYOS_ROOT).
					replace("~", System.getenv("HOME"));

			//TinyOS environment variables
			//TODO: Need to figure out which env vars are needed for mig. For now,
			//eclipse must be invoked from terminal for it to work.
			this.tinyOSEnvVars = new String[] {
					"MOTECOM=serial@"+serialPort,
					"SERIAL_PORT="+serialPort,
					"TOSROOT="+tosRootDir,
					"TOSDIR="+tosRootDir+"/tos",
					"MAKERULES="+tosRootDir+"/support/make/Makerules",
					"PYTHONPATH=.:"+tosRootDir+"/support/sdk/python",
					"CLASSPATH=.:"+tosRootDir+"/support/sdk/java/tinyos.jar",
					"PATH="+System.getenv("PATH")+":"+tosRootDir+"/bin:"+
					tosRootDir+"/support/sdk/c:"+
					"/opt/local/bin:/opt/local/sbin:/Library/Java/Home/bin"+
					":/Users/ixent/Documents/workspaces/SNCBworkspace/SNEE/compiler/src/main/resources/etc/sncb/tools/python/utils:"+
					"/Users/ixent/Documents/workspaces/SNCBworkspace/SNEE/compiler/src/main/resources/etc/sncb/tools/python"};
//					"PATH=/opt/local/bin:/opt/local/sbin:/usr/local/mysql/bin:/bin:/Library/Java/Home/bin:/Users/ixent/work/tinyos-2.x/bin:/Users/ixent/work/tinyos-2.x/support/sdk/c:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/usr/local/git/bin:/usr/X11/bin:/Users/ixent/Documents/workspace/qos-aware/scripts/batch:/Users/ixent/Documents/workspace/qos-aware/scripts/utils:/Users/ixent/Documents/workspace/qos-aware/scripts/qos-exp",
//					"JAVA_HOME=/Library/Java/Home",
//					"JAVAHOME=/usr/bin"};

			this.combinedImage = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}
	

	
	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER init()");
		try {
			logger.trace("Forming network and collecting metadata");
			System.out.println("Forming network and collecting metadata");
			
			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/init");
			String params[] = {pythonScript, topFile, resFile};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN init()");
	}
	

	@Override
	public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep, String queryOutputDir, CostParameters costParams) 
	throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER register()");
		SerialPortMessageReceiver mr;
		try {
			if (demoMode) {
				System.out.println("Query compilation complete.\n");
				System.in.read();
			}
				
			logger.trace("Generating TinyOS/nesC code for query plan.");
			System.out.println("Generating TinyOS/nesC code for query plan.");
			generateNesCCode(qep, queryOutputDir, costParams);

			if (demoMode) {
				System.out.println("nesC code generation complete.\n");
				System.in.read();
			}
				
			logger.trace("Compiling TinyOS/nesC code into executable images.");
			System.out.println("Compiling TinyOS/nesC code into executable images.");
			compileNesCCode(queryOutputDir);
			
			if (demoMode) {
				System.out.println("nesC code compilation complete.\n");
				System.in.read();
			}
				
			logger.trace("Disseminating Query Plan images");
			System.out.println("Disseminating Query Plan images");
			disseminateQueryPlanImages(qep, queryOutputDir);
			
			if (demoMode) {
				System.out.println("Query plan image dissemination complete.\n");
				System.in.read();
			}
				
			logger.trace("Setting up result collector");
			System.out.println("Setting up result collector");
			mr = setUpResultCollector(qep, queryOutputDir);
			
			if (demoMode) {
				System.out.println("Serial port listener for query results ready.");
				System.in.read();
			}
				
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN register()");
		return mr;
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
		boolean useStartUpProtocol = false;
		boolean enableLeds = true;
		boolean usePowerManagement = false;
		boolean deliverLast = false;
		boolean adjustRadioPower = false;
		boolean includeDeluge = false;
		boolean debugLeds = true;
		boolean showLocalTime = false;
		boolean useNodeController = false;
		
		try {
			useNodeController = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
		} catch (SNEEConfigurationException e) {
			// Using the default setting...
			e.printStackTrace();
		}
		TinyOSGenerator codeGenerator = new TinyOSGenerator(tosVersion, tossimFlag, 
			    targetName, combinedImage, queryOutputDir, costParams, controlRadioOff,
			    enablePrintf, useStartUpProtocol, enableLeds,
			    usePowerManagement, deliverLast, adjustRadioPower,
			    includeDeluge, debugLeds, showLocalTime, useNodeController);
		//TODO: in the code generator, need to connect controller components to query plan components
		codeGenerator.doNesCGeneration(qep);		
	}
	
	private void compileNesCCode(String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER compileNesCCode()");
		String nescOutputDir = System.getProperty("user.dir")+"/"+
		queryOutputDir+"tmotesky_t2";
		String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/utils/compileNesCCode.py");
		String nescDirParam = "--nesc-dir="+nescOutputDir;
		String params[] = {pythonScript, nescDirParam};
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		if (logger.isTraceEnabled())
			logger.trace("RETURN compileNesCCode()");
	}

	
	private SerialPortMessageReceiver setUpResultCollector(SensorNetworkQueryPlan qep, 
	String queryOutputDir) throws Exception {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setUpResultCollector()");
		//TODO: need to set up plumbing for query result collection (using mig?)
		String nescOutputDir = System.getProperty("user.dir")+"/"+
			queryOutputDir+"tmotesky_t2";
		String nesCHeaderFile = nescOutputDir+"/mote"+qep.getGateway()+"/QueryPlan.h";
		System.out.println(nesCHeaderFile);
		String outputJavaFile = System.getProperty("user.dir")+"/"+queryOutputDir+"DeliverMessage.java";
		String params[] = {"java", "-target=null", "-java-classname=DeliverMessage",
				nesCHeaderFile, "DeliverMessage", "-o", outputJavaFile};
		Utils.runExternalProgram("mig", params, this.tinyOSEnvVars);
		String deliverMessageJavaClassContent = Utils.readFileToString(outputJavaFile);
		logger.trace("deliverMessageJavaClassContent="+deliverMessageJavaClassContent);
//		logger.trace("Using null;");
//		ClassLoader parentClassLoader = null;
		logger.trace("Using this.getClass().getClassLoader();");
		ClassLoader parentClassLoader = this.getClass().getClassLoader();
//		logger.trace("Using Thread.currentThread().getContextClassLoader();");
//		ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
//		logger.trace("Using parentClassLoader=ClassLoader.getSystemClassLoader()");
//		ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
//		String messageJavaClassContent = Utils.readFileToString(
//				System.getProperty("user.dir")+"/src/mai)");
		MemoryClassLoader mcl = new MemoryClassLoader("DeliverMessage", 
				deliverMessageJavaClassContent, parentClassLoader);
		Class msgClass = mcl.loadClass("DeliverMessage");
		//Class msgClass = Class.forName("DeliverMessage", true, mcl);
		Object msgObj = msgClass.newInstance();
		//Message msg = new DeliverMessage(); // needed for web service, for now.
		Message msg = (Message)msgObj;
		DeliverOperator delOp = (DeliverOperator) qep.getLAF().getRootOperator();
		SerialPortMessageReceiver mr = new SerialPortMessageReceiver("serial@"+this.serialPort+":telos",
				delOp);
		mr.addMsgType(msg);
		if (logger.isTraceEnabled())
			logger.trace("RETURN setUpResultCollector()");	
		return mr;
	}
	
	private void disseminateQueryPlanImages(SensorNetworkQueryPlan qep, 
			String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER disseminateQueryPlanImages()");
		String nescOutputDir = System.getProperty("user.dir")+"/"+
			queryOutputDir+"tmotesky_t2";
		String gatewayID = ""+qep.getGateway();
		Iterator<Site> siteIter = qep.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			String siteID = siteIter.next().getID();
			//skip the gateway
			if (siteID.equals(gatewayID)) 
				continue;
			logger.trace("Imaging mote "+siteID);
			System.out.println("Imaging mote "+siteID);
			String imageFile = nescOutputDir+"/mote"+siteID+"/build/telosb/tos_image.xml";
			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/register");
			String params[] = {pythonScript, imageFile, siteID, gatewayID};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		}
		//do the basestation last
		logger.trace("Imaging basestastion");
		System.out.println("Imaging basestation");
		String imageFile = nescOutputDir+"/mote"+gatewayID+"/build/telosb/tos_image.xml";
		String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/register");
		String params[] = {pythonScript, imageFile, gatewayID, gatewayID};
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		
		if (logger.isTraceEnabled())
			logger.trace("RETURN disseminateQueryPlanImages()");
	}
	
	@Override
	public void start() throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER start()");
		try {
			logger.trace("Invoking start command");
			System.out.println("Invoking start command");
			
			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/start");
			String params[] = {pythonScript};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		} catch (IOException e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN start()");
	}

	@Override
	public void stop(SensorNetworkQueryPlan qep) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER stop()");
		try {
			logger.trace("Invoking stop command");
			System.out.println("Invoking stop command");
			
			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/stop");
			String gatewayID = ""+qep.getGateway();
			String params[] = {pythonScript, gatewayID};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);			
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN stop()");
	}

	@Override
	public void deregister(SensorNetworkQueryPlan qep) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER deregister()");
		try {
			logger.trace("Invoking deregister command");
			System.out.println("Invoking deregister command");
			
			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/deregister");
			Iterator<Site> siteIter = qep.siteIterator(TraversalOrder.POST_ORDER);
			StringBuffer siteString = new StringBuffer();
			while (siteIter.hasNext()) {
				Site site = siteIter.next();
				siteString.append(site.getID()+" ");
			}
			String params[] = {pythonScript, siteString.toString()};
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);			
		}
		if (logger.isDebugEnabled())
		logger.debug("RETURN deregister()");
	}
}
