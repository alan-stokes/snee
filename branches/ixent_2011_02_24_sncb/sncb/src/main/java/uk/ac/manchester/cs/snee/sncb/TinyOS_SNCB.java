package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.tinyos.message.Message;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public class TinyOS_SNCB implements SNCB {

	private Logger logger = Logger.getLogger(TinyOS_SNCB.class.getName());

	private Map<String, String> tinyOSEnvVars;

	private String workingDir;

	private boolean combinedImage = false;

	private String serialPort;

	private boolean demoMode = false;

	private boolean useNodeController = true;

	private CodeGenTarget target = CodeGenTarget.TELOSB_T2; //default
	
	private String targetDirName; 

	// Is the network running?
	private static boolean isStarted = false;
	private SerialPortMessageReceiver mr;

	public TinyOS_SNCB() throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			// TinyOS environment variables
			this.tinyOSEnvVars = new HashMap<String, String>();
			workingDir = Utils.getResourcePath("etc/sncb/tools/python");
			String currentPath = System.getenv("PATH");
			this.tinyOSEnvVars.put("PATH", currentPath + ":" + workingDir + ":"
					+ workingDir + "/utils");
			
			//Check if a mote is plugged in.  If no mote is plugged in, this is null.
			this.serialPort = this.getBaseStation();
			
			// Check if we are using the node controller
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER)) {
				useNodeController = SNEEProperties
				.getBoolSetting(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);				
			}
			// Check whether to generate combined image or individual image
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE)) {
				this.combinedImage = SNEEProperties
					.getBoolSetting(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
			}
			// Parse the code generation target
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET)) {
				this.target = CodeGenTarget.parseCodeTarget(SNEEProperties
					.getSetting(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET));
			}
			// Node controller is only compatible with Tmote Sky/Tiny OS2
			if (this.target != CodeGenTarget.TELOSB_T2 && useNodeController) {
				logger.warn("Node controller is only compatible with Tmote Sky/Tiny OS2. " +
						"Excluding controller from generated code.");
				useNodeController = false;
			}
			targetDirName = target.toString().toLowerCase();
			
			//More TinyOS environment variables
			if (serialPort != null) {
				this.tinyOSEnvVars.put("MOTECOM", "=serial@" + serialPort);
				this.tinyOSEnvVars.put("SERIAL_PORT", serialPort);
			}
		} catch (Exception e) {
			//If an error occurs (e.g., TinyOS is not installed so motelist command fails) serialPort is null.
			this.serialPort = null;
			this.target = CodeGenTarget.TELOSB_T2;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}

	private String getBaseStation() throws SNCBException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER getBaseStation()");
		if (!this.useNodeController) {
			return null;
		}
		String serialPort;
		try {
			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/utils/basestation.py");
			String params[] = { pythonScript };
			String outputLines = Utils.runExternalProgram("python", params,
					this.tinyOSEnvVars, workingDir);
			String outputList[] = outputLines.split("\n");
			if (outputList.length < 2) {
				System.out.println("No basestation found.\n\n");
				return null;
			} else if (outputList.length > 2) { // FIXME: This doesn't work.
				throw new SNCBException(
						"Unable to determine base station mote, as more than one mote is plugged in.");
			}
			serialPort = outputList[1];
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage(), e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN getBaseStation()");
		return serialPort;
	}

	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER init()");
		try {
			logger.trace("Forming network and collecting metadata");
			System.out.println("Forming network and collecting metadata");

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/init");
			String params[] = { pythonScript, topFile, resFile };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN init()");
	}

	@Override
	public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
			String queryOutputDir, CostParameters costParams)
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
			System.out
					.println("Compiling TinyOS/nesC code into executable images.");
			compileNesCCode(queryOutputDir);

			if (demoMode) {
				System.out.println("nesC code compilation complete.\n");
				System.in.read();
			}

			if (!this.useNodeController || this.serialPort==null) {
				System.out.println("Not using node controller, or no mote "+
						"plugged in, so unable to send query plan using" +
						"Over-the-air Programmer. ");
				System.out.println("Please proceed using manual commands.\n");
				if (this.target == CodeGenTarget.TELOSB_T2) {
					TinyOS_SNCB_Utils.printTelosBCommands(queryOutputDir, qep,
							this.targetDirName, this.serialPort);
				} else if (this.target == CodeGenTarget.TOSSIM_T2) {
					TinyOS_SNCB_Utils.printTossimCommands(queryOutputDir,
							this.targetDirName);
				} else if (this.target == CodeGenTarget.AVRORA_MICA2_T2 ||
						this.target == CodeGenTarget.AVRORA_MICAZ_T2) {
					TinyOS_SNCB_Utils.printAvroraCommands(queryOutputDir, qep, 
							this.targetDirName, this.target);					
				}
				System.exit(0);
			}

			logger.trace("Disseminating Query Plan images");
			System.out.println("Disseminating Query Plan images");
			disseminateQueryPlanImages(qep, queryOutputDir);

			if (demoMode) {
				System.out
						.println("Query plan image dissemination complete.\n");
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
			logger.warn(e.getLocalizedMessage(), e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN register()");
		return mr;
	}

	
	private void generateNesCCode(SensorNetworkQueryPlan qep,
			String queryOutputDir, CostParameters costParams)
			throws IOException, SchemaMetadataException, TypeMappingException,
			OptimizationException, CodeGenerationException {
		// TODO: move some of these to an sncb .properties file
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

		TinyOSGenerator codeGenerator = new TinyOSGenerator(target, combinedImage, queryOutputDir,
				costParams, controlRadioOff, enablePrintf, useStartUpProtocol,
				enableLeds, usePowerManagement, deliverLast, adjustRadioPower,
				includeDeluge, debugLeds, showLocalTime, useNodeController);
		// TODO: in the code generator, need to connect controller components to
		// query plan components
		codeGenerator.doNesCGeneration(qep);
	}

	private void compileNesCCode(String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER compileNesCCode()");
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + targetDirName;
		String pythonScript = Utils
				.getResourcePath("etc/sncb/tools/python/utils/compileNesCCode.py");
		String nescDirParam = "--nesc-dir=" + nescOutputDir;
		String targetDirNameParam = "--compile-target=" + targetDirName;
		String params[] = { pythonScript, nescDirParam, targetDirNameParam };
		Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
				workingDir);
		if (logger.isTraceEnabled())
			logger.trace("RETURN compileNesCCode()");
	}

	private SerialPortMessageReceiver setUpResultCollector(
			SensorNetworkQueryPlan qep, String queryOutputDir) throws Exception {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setUpResultCollector()");
		// TODO: need to set up plumbing for query result collection (using
		// mig?)
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + targetDirName;
		String nesCHeaderFile = nescOutputDir + "/mote" + qep.getGateway()
				+ "/QueryPlan.h";
		System.out.println(nesCHeaderFile);
		String outputJavaFile = System.getProperty("user.dir") + "/"
				+ queryOutputDir + "DeliverMessage.java";
		String params[] = { "java", "-target=null",
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
		Class msgClass = mcl.loadClass("DeliverMessage");
		// Class msgClass = Class.forName("DeliverMessage", true, mcl);
		Object msgObj = msgClass.newInstance();
		// Message msg = new DeliverMessage(); // needed for web service, for
		// now.
		Message msg = (Message) msgObj;
		SensornetDeliverOperator delOp = (SensornetDeliverOperator) qep.getDAF()
				.getRootOperator();
		mr = new SerialPortMessageReceiver("serial@"
				+ this.serialPort + ":telos", delOp);
		mr.addMsgType(msg);
		if (logger.isTraceEnabled())
			logger.trace("RETURN setUpResultCollector()");
		return mr;
	}

	private void disseminateQueryPlanImages(SensorNetworkQueryPlan qep,
			String queryOutputDir) throws IOException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER disseminateQueryPlanImages()");
		String nescOutputDir = System.getProperty("user.dir") + "/"
				+ queryOutputDir + targetDirName;
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

	@Override
	public void start() throws SNCBException {
		// Call python start script
		if (logger.isDebugEnabled())
			logger.debug("ENTER start()");
		try {
			logger.trace("Invoking start command");
			System.out.println("Invoking start command");

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/start");
			String params[] = { pythonScript };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		} catch (IOException e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		
		isStarted = true;

		// Kick off a thread to do synchronisation
		(new Thread(new Runnable() {

			@Override
			public void run() {
				// loop continuously until the stop() method is invoked
				while (isStarted) {
					try {
						Thread.sleep(60 * 1000);

						// cycle a stop/start to synchronise nodes. NOTE: This
						// start/stop mechanism does not guarantee
						// synchronisation. It was chosen as a trade-off
						// between code size and accuracy.
						pause();
						resume();
					} catch (Exception e) {
						// General fail case
						System.out.println("Execution failed. See logs for detail.");
						logger.fatal(e);
						System.exit(1);
					}
				}
			}
		})).start();

		if (logger.isDebugEnabled())
			logger.debug("RETURN start()");
	}

	@Override
	public void stop(SensorNetworkQueryPlan qep) throws SNCBException {
		isStarted = false;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER stop()");
		try {
			logger.trace("Invoking stop command");
			System.out.println("Invoking stop command");

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/stop");
			String gatewayID = Integer.toString(qep.getGateway());
			String params[] = { pythonScript, gatewayID };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
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

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/deregister");
			String gatewayID = "" + qep.getGateway();
			Iterator<Site> siteIter = qep
					.siteIterator(TraversalOrder.POST_ORDER);
			StringBuffer siteString = new StringBuffer();
			while (siteIter.hasNext()) {
				String siteID = siteIter.next().getID();
				// skip the gateway
				if (siteID.equals(gatewayID))
					continue;
				siteString.append(siteID + " ");
			}
			String params[] = { pythonScript, siteString.toString() };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN deregister()");
	}

	private void pause() throws SNCBException {
		if (!isStarted) return;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER pause()");
		
//		// release the serial port		
//		mr.pause();
		
		try {
			logger.trace("Invoking pause command");
			System.out.println("Invoking pause command");

			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/pause");
			String params[] = { pythonScript };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN pause()");
	}

	private void resume() throws SNCBException {
		if (!isStarted) return;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER resume()");
		
		try {
			logger.trace("Invoking resume command");
			System.out.println("Invoking resume command");

			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/resume");
			String params[] = { pythonScript };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
				
//		// reclaim the serial port
//		mr.resume();
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN resume()");
	}
}
