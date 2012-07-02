package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.tinyos.message.Message;

import org.apache.log4j.Logger;


import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

public abstract class TinyOS_SNCB implements SNCB {

	/**
	 * 
	 */
	private static final long serialVersionUID = -547558400665610779L;

	protected static final Logger logger = Logger.getLogger(TinyOS_SNCB.class.getName());

	protected Map<String, String> tinyOSEnvVars;

	protected String workingDir;

	protected boolean combinedImage = false;

	protected String serialPort = null;

	protected boolean demoMode = false;

	protected boolean useNodeController = false;

	protected CodeGenTarget target = null;

	protected boolean controlRadio = false;

	// Is the network running?
	protected static boolean isStarted = false;
	protected SerialPortMessageReceiver mr;

	protected double duration;
	
	public TinyOS_SNCB(double duration) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			this.duration = duration;
			// TinyOS environment variables
			this.tinyOSEnvVars = new HashMap<String, String>();
			workingDir = Utils.getResourcePath("etc/sncb/tools/python");
			String currentPath = System.getenv("PATH");
			this.tinyOSEnvVars.put("PATH", currentPath + ":" + workingDir + ":"
					+ workingDir + "/utils");
			// Check whether to generate combined image or individual image
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE)) {
				this.combinedImage = SNEEProperties
					.getBoolSetting(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
			}			
		} catch (Exception e) {
			//If an error occurs (e.g., TinyOS is not installed so motelist command fails) serialPort is null.
			this.serialPort = null;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}



	@Override
	public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
			String queryOutputDir, MetadataManager metadata)
			throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER register()");
		try {
			if (demoMode) {
				System.out.println("Query compilation complete.\n");
				System.in.read();
			}

			logger.trace("Generating TinyOS/nesC code for query plan.");
			System.out.println("Generating TinyOS/nesC code for query plan.");
			generateNesCCode(qep, queryOutputDir, metadata);

			if (demoMode) {
				System.out.println("nesC code generation complete.\n");
				System.in.read();
			}

			logger.trace("Compiling TinyOS/nesC code into executable images.");
			System.out
					.println("Compiling TinyOS/nesC code into executable images.");
			compileNesCCode(queryOutputDir);

			if (demoMode) {
				System.out.println("nesC code compilation complete.\n");
				System.in.read();
			}
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage(), e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN register()");
		return null;
	}

	
	public void generateNesCCode(SensorNetworkQueryPlan qep,
			String queryOutputDir, MetadataManager metadata)
			throws IOException, SchemaMetadataException, TypeMappingException,
			OptimizationException, CodeGenerationException {
		// TODO: move some of these to an sncb .properties file
		boolean enablePrintf = false; //TODO: not tested
		boolean enableLeds = true;
		boolean debugLeds = true;

		TinyOSGenerator codeGenerator = new TinyOSGenerator(target, combinedImage, queryOutputDir,
				metadata, controlRadio, enablePrintf, enableLeds,
				debugLeds, useNodeController);
		codeGenerator.doNesCGeneration(qep);
	}

	public void compileNesCCode(String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER compileNesCCode()");
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + this.getTargetDirName();
		String pythonScript = Utils
				.getResourcePath("etc/sncb/tools/python/utils/compileNesCCode.py");
		String nescDirParam = "--nesc-dir=" + nescOutputDir;
		String targetDirNameParam = "--compile-target=" + this.getTargetDirName();
		String params[] = { pythonScript, nescDirParam, targetDirNameParam };
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
				workingDir);
		if (logger.isTraceEnabled())
			logger.trace("RETURN compileNesCCode()");
	}
	
	public void compileReducedNesCCode(String queryOutputDir, ArrayList<String> NodeIds) throws IOException {
    if (logger.isTraceEnabled())
      logger.trace("ENTER compileNesCCode()");
    String nescOutputDir = System.getProperty("user.dir") + "/"
        + queryOutputDir + this.getTargetDirName();
    String pythonScript = Utils
        .getResourcePath("etc/sncb/tools/python/utils/compileReducedNesCCode.py");
    String nescDirParam = "--nesc-dir=" + nescOutputDir;
    String targetDirNameParam = "--compile-target=" + this.getTargetDirName();
    ArrayList<String> parameters = new ArrayList<String>();
    parameters.add(pythonScript);
     parameters.add(nescDirParam);
    parameters.add(targetDirNameParam);
    Iterator<String> nodeIdIterator = NodeIds.iterator();
    while(nodeIdIterator.hasNext())
    {
      String nodeID = nodeIdIterator.next();
      nodeID = "mote" + nodeID;
      parameters.add(nodeID);
    }
    String params[] = new String [NodeIds.size() + 3];
    parameters.toArray(params);
    Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
        workingDir);
    if (logger.isTraceEnabled())
      logger.trace("RETURN compileNesCCode()");
  }

	protected SerialPortMessageReceiver setUpResultCollector(
			SensorNetworkQueryPlan qep, String queryOutputDir) throws Exception {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setUpResultCollector()");
		// TODO: need to set up plumbing for query result collection (using
		// mig?)
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + this.getTargetDirName();
		String nesCHeaderFile = nescOutputDir + "/mote" + qep.getGateway()
				+ "/QueryPlan.h";
		System.out.println(nesCHeaderFile);
		String outputJavaFile = System.getProperty("user.dir") + "/"
				+ queryOutputDir + "DeliverMessage.java";
		String params[] = { "java", "-target=telosb",
				"-java-classname=DeliverMessage", nesCHeaderFile,
				"DeliverMessage", "-o", outputJavaFile };
		Utils.runExternalProgram("mig", params, this.tinyOSEnvVars, workingDir);
		String deliverMessageJavaClassContent = Utils
				.readFileToString(outputJavaFile);
		logger.trace("deliverMessageJavaClassContent="
				+ deliverMessageJavaClassContent);
		// logger.trace("Using null;");
		// ClassLoader parentClassLoader = null;
		logger.trace("Using this.getClass().getClassLoader();");
		ClassLoader parentClassLoader = this.getClass().getClassLoader();
		// logger.trace("Using Thread.currentThread().getContextClassLoader();");
		// ClassLoader parentClassLoader =
		// Thread.currentThread().getContextClassLoader();
		// logger.trace("Using parentClassLoader=ClassLoader.getSystemClassLoader()");
		// ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
		// String messageJavaClassContent = Utils.readFileToString(
		// System.getProperty("user.dir")+"/src/mai)");
		MemoryClassLoader mcl = new MemoryClassLoader("DeliverMessage",
				deliverMessageJavaClassContent, parentClassLoader);
		Class<?> msgClass = mcl.loadClass("DeliverMessage");
		// Class msgClass = Class.forName("DeliverMessage", true, mcl);
		Object msgObj = msgClass.newInstance();
		// Message msg = new DeliverMessage(); // needed for web service, for
		// now.
		Message msg = (Message) msgObj;
		SensornetDeliverOperator delOp = (SensornetDeliverOperator) qep.getDAF()
				.getRootOperator();
		if (this.serialPort != null) {
			mr = new SerialPortMessageReceiver("serial@"
					+ this.serialPort + ":telos", delOp);
		} else {
			mr = new SerialPortMessageReceiver(null, delOp);
		}
		mr.addMsgType(msg);
		if (logger.isTraceEnabled())
			logger.trace("RETURN setUpResultCollector()");
		return mr;
	}

	protected void disseminateQueryPlanImages(SensorNetworkQueryPlan qep,
			String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER disseminateQueryPlanImages()");
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + this.getTargetDirName();
		String gatewayID = "" + qep.getGateway();
		Iterator<Site> siteIter = qep.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			String siteID = siteIter.next().getID();
			// skip the gateway
			if (siteID.equals(gatewayID))
				continue;
			logger.trace("Imaging mote " + siteID);
			System.out.println("Imaging mote " + siteID);
			String imageFile = nescOutputDir + "/mote" + siteID
					+ "/build/telosb/tos_image.xml";
			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/register");
			String params[] = { pythonScript, imageFile, siteID, gatewayID };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		}
		// do the basestation last
		logger.trace("Imaging basestastion");
		System.out.println("Imaging basestation");
		String imageFile = nescOutputDir + "/mote" + gatewayID
				+ "/build/telosb/tos_image.xml";
		String pythonScript = Utils
				.getResourcePath("etc/sncb/tools/python/register");
		String params[] = { pythonScript, imageFile, gatewayID, gatewayID };
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
				workingDir);

		if (logger.isTraceEnabled())
			logger.trace("RETURN disseminateQueryPlanImages()");
	}
	
	protected String getTargetDirName() {
		return target.toString().toLowerCase();
	}
}
