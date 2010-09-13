/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.sncb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;
import uk.ac.manchester.cs.snee.sncb.tos.AMRecieveComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AMSendComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AcquireComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ActiveMessageIDGenerator;
import uk.ac.manchester.cs.snee.sncb.tos.AggrEvalComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AggrInitComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AggrMergeComponent;
import uk.ac.manchester.cs.snee.sncb.tos.CC1000ControlComponent;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenUtils;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.tos.DeliverComponent;
import uk.ac.manchester.cs.snee.sncb.tos.DelugeComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ExchangeProducerComponent;
import uk.ac.manchester.cs.snee.sncb.tos.FragmentComponent;
import uk.ac.manchester.cs.snee.sncb.tos.JoinComponent;
import uk.ac.manchester.cs.snee.sncb.tos.LedComponent;
import uk.ac.manchester.cs.snee.sncb.tos.LocalTimeComponent;
import uk.ac.manchester.cs.snee.sncb.tos.MainComponent;
import uk.ac.manchester.cs.snee.sncb.tos.NesCComponent;
import uk.ac.manchester.cs.snee.sncb.tos.NesCConfiguration;
import uk.ac.manchester.cs.snee.sncb.tos.PowerManagementComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ProjectComponent;
import uk.ac.manchester.cs.snee.sncb.tos.QueryPlanModuleComponent;
import uk.ac.manchester.cs.snee.sncb.tos.RXT1Component;
import uk.ac.manchester.cs.snee.sncb.tos.RXT2Component;
import uk.ac.manchester.cs.snee.sncb.tos.RadioComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SelectComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SensorT1Component;
import uk.ac.manchester.cs.snee.sncb.tos.SensorT2Component;
import uk.ac.manchester.cs.snee.sncb.tos.SerialAMReceiveComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SerialAMSendComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SerialComponent;
import uk.ac.manchester.cs.snee.sncb.tos.TXT1Component;
import uk.ac.manchester.cs.snee.sncb.tos.TXT2Component;
import uk.ac.manchester.cs.snee.sncb.tos.Template;
import uk.ac.manchester.cs.snee.sncb.tos.TimerT1Component;
import uk.ac.manchester.cs.snee.sncb.tos.TimerT2Component;
import uk.ac.manchester.cs.snee.sncb.tos.TrayComponent;
import uk.ac.manchester.cs.snee.sncb.tos.WindowComponent;

/**
 *
 * Code Generation: step 8 of query compilation.
 *
 * <i>Code Generation</i> generates executable code for each site based on the
 * distributed QEP, RT and the agenda.
 *
 * @author Ixent Galpin, Farhana Jabeen, Christian Brenninkmeijer
 *
 */
public class TinyOSGenerator {

    Logger logger = Logger.getLogger(TinyOSGenerator.class.getName());

	/**
	 * The directory where nesC interfaces, used as input to the generator,
	 * are located.
	 */
	public static String NESC_TEMPLATES_DIR;

	/**
	 * The directory where nesC interfaces, used as input to the generator,
	 * are located.
	 * This is derived from NESC_TEMPLATES_DIR
	 */
	public static String NESC_INTERFACES_DIR;

	/**
	 * The directory where nesC module templates, used as input to the
	 * generator, are located.
	 * This is derived from NESC_TEMPLATES_DIR
	 */
	public static String NESC_COMPONENTS_DIR;

	/**
	 * The directory where nesC misc files, used as input to the
	 * generator, are located.
	 * This is derived from NESC_TEMPLATES_DIR
	 */
	public static String NESC_MISC_FILES_DIR;


	/**
	 * Constants for the components and interfaces used
     * TinyOS 1 and 2
	 */
    public static String COMPONENT_RADIO;

    private static String COMPONENT_MAIN;

    public static String COMPONENT_QUERY_PLAN;

    private static String COMPONENT_QUERY_PLANC;

    public static String COMPONENT_SENSOR;

    private static String COMPONENT_LEDS;
    
    public static String INTERFACE_DO_TASK;

    public static String INTERFACE_GET_TUPLES;

    public static String INTERFACE_PUT_TUPLES;

    public static String INTERFACE_RECEIVE;

    public static String INTERFACE_SEND;

    public static String INTERFACE_TIMER;

    private static String INTERFACE_READ;

    // TinyOS 1 only
    private static String COMPONENT_TIMER;

    private static String COMPONENT_POWER_MANAGEMENT;

    private static String COMPONENT_CC1000CONTROL;

    private static String INTERFACE_SENSOR;

    public static String INTERFACE_STDCONTROL;

    private static String INTERFACE_LEDS;

    private static String INTERFACE_CC1000CONTROL;

    // TinyOS2 Only
    private static String COMPONENT_AGENDA_TIMER;

    private static String COMPONENT_LOCAL_TIME;
    
    private static String COMPONENT_RADIOTX;

    private static String COMPONENT_RADIORX;

    public static String COMPONENT_SERIAL_DEVICE;
    
    public static String COMPONENT_SERIALTX;
    
    public static String COMPONENT_SERIALRX;

    public static String COMPONENT_DELUGE;
    
    private static String INTERFACE_AMPACKET;
    
    private static String INTERFACE_BOOT;

    public static String INTERFACE_LOCAL_TIME;

    private static String INTERFACE_PACKET;

    public static String INTERFACE_SNOOZE;

    public static String INTERFACE_SPLITCONTROL;
    
    public static String TYPE_TMILLI;

    public static String TYPE_READ;

    
    
    private SensorNetworkQueryPlan plan;
    
    private int tosVersion = 2;
    
    private boolean tossimFlag = true;
    
    private String targetName;
    
    private boolean combinedImage = false;
    
    private String nescOutputDir;
    
    private CostParameters costParams;
    
	private boolean controlRadioOff;

	private boolean enablePrintf;

	private boolean useStartUpProtocol;

	private boolean enableLeds;

	private boolean usePowerManagement;

	private boolean deliverLast;

	private boolean adjustRadioPower;
	
	private boolean includeDeluge;

	private boolean debugLeds;
	
	private boolean showLocalTime; //Not working
    
    public TinyOSGenerator(int tosVersion, boolean tossimFlag, 
    String targetName,
    boolean combinedImage, String nescOutputDir, CostParameters costParams, boolean controlRadioOff,
    boolean enablePrintf, boolean useStartUpProtocol, boolean enableLeds,
    boolean usePowerManagement, boolean deliverLast, boolean adjustRadioPower,
    boolean includeDeluge, boolean debugLeds, boolean showLocalTime)
    throws IOException, SchemaMetadataException, TypeMappingException {
    	this.tosVersion = tosVersion;
    	this.tossimFlag = tossimFlag;
    	this.targetName = targetName;
    	this.combinedImage = combinedImage;
    	this.nescOutputDir = nescOutputDir;
		this.costParams = costParams;

		this.controlRadioOff =controlRadioOff;
		this.enablePrintf = enablePrintf;
		this.useStartUpProtocol = useStartUpProtocol;
		this.enableLeds = enableLeds;
		this.usePowerManagement = usePowerManagement;
		this.deliverLast = deliverLast;
		this.adjustRadioPower = adjustRadioPower;
		this.includeDeluge = includeDeluge;
		this.debugLeds = debugLeds;
		this.showLocalTime = showLocalTime;
    	
    	initConstants(tosVersion, tossimFlag, targetName);

    	if (tosVersion == 1) {
    		NESC_TEMPLATES_DIR = "etc/sncb/tos1/"; 
    	} else {
    		NESC_TEMPLATES_DIR = "etc/sncb/tos2/"; 
    	}
    	NESC_COMPONENTS_DIR = NESC_TEMPLATES_DIR + "components";
    	NESC_INTERFACES_DIR = NESC_TEMPLATES_DIR + "interfaces";
    	NESC_MISC_FILES_DIR = NESC_TEMPLATES_DIR + "misc";
    }

    /**
     * Initialises the constants which specify the component names. These vary
     * depending on whether we are generating TinyOS1/TinyOS2 nesC code, and
     * whether it is for the Tossim simulator or not.
     *
     * @param tossimFlag	Specifies whether Tossim code is being generated.
     * @param targetName 
     */
    private static void initConstants(int tosVersion, boolean tossimFlag, String targetName) {
		if (tosVersion == 1) {
		    COMPONENT_RADIO = "GenericComm";
		    COMPONENT_MAIN = "Main";
		    COMPONENT_QUERY_PLAN = "QueryPlanM";
		    COMPONENT_QUERY_PLANC = "QueryPlan";
		    COMPONENT_DELUGE = "DelugeC";
		    if (!tossimFlag) {
		    	COMPONENT_SENSOR = "PhotoTemp";
		    } else {
		    	COMPONENT_SENSOR = "ADCC";
		    }
		    COMPONENT_TIMER = "TimerC";
		    COMPONENT_LEDS = "LedsC";
		    COMPONENT_POWER_MANAGEMENT = "HPLPowerManagementM";
		    COMPONENT_CC1000CONTROL = "CC1000ControlM";
		    INTERFACE_DO_TASK = "DoTask";
		    INTERFACE_GET_TUPLES = "GetTuples";
		    INTERFACE_PUT_TUPLES = "PutTuples";
		    INTERFACE_RECEIVE = "ReceiveMsg";
		    INTERFACE_SEND = "SendMsg";
		    INTERFACE_TIMER = "Timer";
		    INTERFACE_READ = "ADC";
		    INTERFACE_SENSOR = "ExternalPhotoADC";
		    INTERFACE_STDCONTROL = "StdControl";
		    INTERFACE_LEDS = "Leds";
		    INTERFACE_CC1000CONTROL = "CC1000Control";
		} else {
		    COMPONENT_AGENDA_TIMER = "AgendaTimer";
		    COMPONENT_DELUGE = "DelugeC";
		    COMPONENT_LOCAL_TIME = "LocalTimeMilliC";
		    COMPONENT_MAIN = "MainC";
		    COMPONENT_QUERY_PLAN = "QueryPlanC";
		    COMPONENT_QUERY_PLANC = "QueryPlan";  //AppC
		    COMPONENT_RADIO = "ActiveMessageC";
		    COMPONENT_RADIOTX = "AMSenderC";
		    COMPONENT_RADIORX = "AMRecieverC";
		    if (targetName.equals("tmotesky_t2")) {
		    	COMPONENT_SENSOR = "HamamatsuS1087ParC";
		    } else if (tossimFlag) {
		    	COMPONENT_SENSOR  = "RandomSensorC";
		    } else {
		    	COMPONENT_SENSOR = "DemoSensorC";
		    }
		    	
		    COMPONENT_LEDS = "LedsC";
		    COMPONENT_SERIAL_DEVICE = "SerialActiveMessageC";
		    COMPONENT_SERIALTX = "SerialAMSenderC";
		    COMPONENT_SERIALRX = "SerialAMReceiverC";		    
		    INTERFACE_DO_TASK = "DoTask";
		    INTERFACE_GET_TUPLES = "GetTuples";
		    INTERFACE_LOCAL_TIME = "LocalTime";
		    INTERFACE_PUT_TUPLES = "PutTuples";
		    INTERFACE_RECEIVE = "Receive";
		    INTERFACE_SEND = "AMSend";
		    INTERFACE_TIMER = "Timer";
		    INTERFACE_BOOT = "Boot";
		    INTERFACE_PACKET = "Packet";
		    INTERFACE_AMPACKET = "AMPacket";
		    INTERFACE_READ = "Read";
		    INTERFACE_SNOOZE = "Snooze";
		    INTERFACE_LEDS = "Leds";
		    INTERFACE_SPLITCONTROL = "SplitControl";
		    TYPE_TMILLI = "TMilli";
		    TYPE_READ = "uint16_t";
		}
    }


    /**
     * Precompute the operator tuple sizes and store in hashtable.
     * @param plan The query plan to compute tuples sizes for.
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    private void computeOperatorTupleSizes() throws SchemaMetadataException, TypeMappingException {

		final Iterator<SensornetOperator> opIter = plan.getDAF().
			operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = (SensornetOperator) opIter.next();

		    final List<Attribute> attributes = op.getAttributes();
		    int tupleSize = 0;

		    for (int i = 0; i<attributes.size();i++) {
		    	final AttributeType attrType = attributes.get(i).getType();
				tupleSize += attrType.getSize();
		    }
		    assert (tupleSize > 0);

		    if (tupleSize != op.getPhysicalTupleSize()) {
				final OptimizationException e = new OptimizationException(
					"Tuple size calculated by NesCGeneration != tuple size reported by operator");
		    }
		    CodeGenUtils.outputTypeSize.put(CodeGenUtils
			    .generateOutputTupleType(op), new Integer(tupleSize));
		    // store for later
		}
    }


    /**
     * Generates the configurations for each individual site.
     * @param plan  The query plan which code is being generated for.
     * @param qos  The user-specified quality-of-service requirements.
     * @param sink The sink node of the network.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @return
     * @throws IOException
     * @throws CodeGenerationException
     */
    private HashMap<Site, NesCConfiguration> generateSiteConfigurations() throws IOException,
	    CodeGenerationException {
    	
    	HashMap<Site, NesCConfiguration> siteConfigs = new HashMap<Site, NesCConfiguration>();
		if (tosVersion == 1) {
		    siteConfigs = t1GenerateSiteConfigs();
		} else {
			siteConfigs = t2GenerateSiteConfigs();
		}

		return siteConfigs;
    }


    /**
     * Add the LED component to the site configurations, and wire each component to it.
     * @param siteConfigs
     * @param tosVersion
     * @param tossimFlag
     * @throws CodeGenerationException
     */
    private void addLEDs(HashMap<Site, NesCConfiguration> siteConfigs, 
    		NesCConfiguration tossimConfig)
    		throws CodeGenerationException {

    	Iterator<Site> siteIter = siteConfigs.keySet().iterator();
    	while (siteIter.hasNext()) {
    		Site site = siteIter.next();
    		NesCConfiguration config = siteConfigs.get(site);
    		addLEDsToOuterConfiguration(config);
    	}
    	
    	addLEDsToOuterConfiguration(tossimConfig);
    }

	private void addLEDsToOuterConfiguration(NesCConfiguration config)
			throws CodeGenerationException {
		LedComponent ledComp = new LedComponent(COMPONENT_LEDS, config, tosVersion, tossimFlag);
		config.addComponent(ledComp);
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_LEDS, INTERFACE_LEDS);
		
		if ((this.includeDeluge) && (tossimFlag == false)) {
			config.addWiring(COMPONENT_DELUGE, COMPONENT_LEDS, INTERFACE_LEDS);
		}
		
		final Iterator<NesCComponent> compIter = config.componentIterator();
		while (compIter.hasNext()) {
		    final NesCComponent comp = compIter.next();

		    if (!comp.isSystemComponent()) {
		    	config.addWiring(comp.getID(), COMPONENT_LEDS, INTERFACE_LEDS);    		    	
		    }
		    
		    if (comp instanceof FragmentComponent) {
		    	FragmentComponent fragComp = (FragmentComponent)comp;
		    	NesCConfiguration fragConfig = fragComp.getInnerConfig();
				final Iterator<NesCComponent> intraFragCompIter = fragConfig.componentIterator();
				while (intraFragCompIter.hasNext()) {
				    final NesCComponent intraFragComp = intraFragCompIter.next();
				    if (!intraFragComp.isSystemComponent()) {				    
				    	fragConfig.linkToExternalProvider(intraFragComp.getID(),
							INTERFACE_LEDS, INTERFACE_LEDS, INTERFACE_LEDS);
				    }
				}
				
		    }
		}
	}
    
    /**
     * TOS1: Generates the top-level configurations for each site in the sensor
     * network.
     * @param plan The query plan which code is being generated for.
     * @param qos The user-specified quality-of-service requirements.
     * @param sink The sink node of the network.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @return
     * @throws IOException
     * @throws CodeGenerationException
     */
    private HashMap<Site, NesCConfiguration> t1GenerateSiteConfigs()
	    throws IOException, CodeGenerationException {

		final HashMap<Site, NesCConfiguration> siteConfigs = new HashMap<Site, NesCConfiguration>();

		int acquireCount = 0;

		final Iterator<Site> siteIter = plan.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			final Site currentSite = siteIter.next();
			final String currentSiteID = currentSite.getID();

			/* Instantiate the top-level site configuration */
			final NesCConfiguration config = new NesCConfiguration(
				COMPONENT_QUERY_PLANC + currentSite.getID(), //%
				plan,
				currentSite,
				tosVersion,
				tossimFlag);

			/* Add the components which are always needed */
			t1AddMainSiteComponents(config);

			/* Add the Power Management components */
			t1AddPowerManagmentComponents(config);

			/* Add functionality for changing radio transmit power */
			t1AddRadioPowerAdjustComponents(config);

			/* Add the DAF fragments allocated to this site */
			acquireCount = t1AddSiteFragments(acquireCount, currentSite, config);

			/* Wire the fragments with trays, radio receive and transmit*/
			wireSiteFragments(currentSite,currentSiteID, config);

			siteConfigs.put(currentSite, config);
		}
		return siteConfigs;
    }

    /**
     * TOS1: Add the components which are always needed a site configuration.
     * @param config The nesC configuration which components/wirings are being added to.
     * @throws CodeGenerationException
     */
	private void t1AddMainSiteComponents(final NesCConfiguration config) throws CodeGenerationException {

		MainComponent mainComp = new MainComponent(COMPONENT_MAIN, config,
			tosVersion, tossimFlag);
		config.addComponent(mainComp);

		//Main query plan class
		int sink = new Integer(plan.getRT().getRoot().getID()).intValue();
		QueryPlanModuleComponent queryPlanModuleComp =
			new QueryPlanModuleComponent(COMPONENT_QUERY_PLAN, config,
			plan, sink, tosVersion, (tossimFlag || combinedImage), targetName,
			costParams, controlRadioOff, enablePrintf, useStartUpProtocol, enableLeds,
			debugLeds, usePowerManagement, deliverLast, adjustRadioPower);
		config.addComponent(queryPlanModuleComp);

		TimerT1Component timerComp = new TimerT1Component( //$
			COMPONENT_TIMER, config, tossimFlag);
		config.addComponent(timerComp);

		//In TinyOS 1 the radio component is assumed to be always
		//required, as it is assumed that a node will transmit over
		//the airwaves or transmit over the serial port or both.  For all
		//the aforementioned cases a radio component is needed.
		final RadioComponent radioComp = new RadioComponent(
			COMPONENT_RADIO, config, tossimFlag);
		config.addComponent(radioComp);
		//Required to turn the radio on/off
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_RADIO,
			INTERFACE_STDCONTROL, "CommControl", "Control");
		
		//Entry point to the query plan code
		config.addWiring(COMPONENT_MAIN, COMPONENT_QUERY_PLAN,
			INTERFACE_STDCONTROL, INTERFACE_STDCONTROL,
			INTERFACE_STDCONTROL);
		//Used to switch on the timer
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_TIMER,
			INTERFACE_STDCONTROL, INTERFACE_TIMER + INTERFACE_STDCONTROL,
			INTERFACE_STDCONTROL);

		//Required for synchronization in TOS1	//$
		if (tossimFlag) {
			config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_TIMER,
					INTERFACE_TIMER, "SyncTimer", INTERFACE_TIMER
						+ "[unique(\"Timer\")]");
		}
		//Used to trigger agenda tasks
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_TIMER,
			INTERFACE_TIMER, "AgendaTimer", INTERFACE_TIMER
				+ "[unique(\"Timer\")]");
		//Adds a delay to give radio time switch on
		if (!tossimFlag && this.controlRadioOff) {
			config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_TIMER,
					INTERFACE_TIMER, "RadioOnTimer", INTERFACE_TIMER
						+ "[unique(\"Timer\")]");
		}
	}

	/**
	 * TOS1: Add the Power Management components.
	 * @param tosVersion The TinyOS version which nesC is being generated for.
	 * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
	 * @param config The nesC confiugration which components/wirings are being added to.
	 * @throws CodeGenerationException
	 */
	private void t1AddPowerManagmentComponents(final NesCConfiguration config)
			throws CodeGenerationException {
		if (this.usePowerManagement) {
			config.addComponent(new PowerManagementComponent(
				COMPONENT_POWER_MANAGEMENT, config, tosVersion, tossimFlag));
			config.addWiring(COMPONENT_QUERY_PLAN,
				COMPONENT_POWER_MANAGEMENT, "command result_t PowerEnable();",
				"PowerEnable", "Enable");
			config.addWiring(COMPONENT_QUERY_PLAN,
				COMPONENT_POWER_MANAGEMENT, "command result_t PowerDisable();",
				"PowerDisable", "Disable");
		}
	}


	/**
	 * TOS1: Add functionality for changing radio transmit power.
	 * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
	 * @param config The nesC confiugration which components/wirings are being added to.
	 * @throws CodeGenerationException
	 */
	private void t1AddRadioPowerAdjustComponents(final NesCConfiguration config) 
	throws CodeGenerationException {
		if (!tossimFlag && this.adjustRadioPower) {
			//Wiring to enable radio transmit power to be adjusted
			final CC1000ControlComponent CC1000ControlComp =
				new CC1000ControlComponent(COMPONENT_CC1000CONTROL, config, tossimFlag);
			config.addComponent(CC1000ControlComp);
			config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_CC1000CONTROL,
					INTERFACE_CC1000CONTROL);
		}
	}


	/**
	 * TOS1: Add the DAF fragments allocated to this site.
	 * @param acquireCount The number of acquire operators encountered on this site.
	 * @param currentSite The site for which code is being generated.
	 * @param config The nesC configuration which components/wirings are being added to.
	 * @return
	 * @throws CodeGenerationException
	 */
	private int t1AddSiteFragments(	int acquireCount, final Site currentSite,
			final NesCConfiguration config) throws CodeGenerationException {
		final Iterator<Fragment> fragIter = currentSite.getFragments()
			.iterator();
		while (fragIter.hasNext()) {
			final Fragment frag = fragIter.next();

			/* Add component for current fragment */
			final FragmentComponent fragComp = new FragmentComponent(
				frag, config, tosVersion, tossimFlag);
			config.addComponent(fragComp);

			/* Wire fragment to main query plan component */
			config.addWiring(COMPONENT_QUERY_PLAN, fragComp.getID(),
				INTERFACE_DO_TASK, CodeGenUtils
					.generateUserAsDoTaskName(frag, currentSite),
				INTERFACE_DO_TASK);

			/* Wire fragment to hardware devices as required */
			acquireCount = t1WireFragToDevices(acquireCount,
					currentSite, config, frag, fragComp);
		}
		return acquireCount;
	}


	/**
	 * TOS1: Wire fragment component to required devices, e.g., serial port or
	 * sensors.
	 * @param acquireCount
	 * @param currentSite
	 * @param config
	 * @param frag
	 * @param fragComp
	 * @return
	 * @throws CodeGenerationException
	 */
	private int t1WireFragToDevices(int acquireCount,
			final Site currentSite, final NesCConfiguration config,
			final Fragment frag, final FragmentComponent fragComp)
			throws CodeGenerationException {
		final Iterator<SensornetOperator> opIter = frag
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
			final SensornetOperator op = opIter.next();
			if (op instanceof SensornetAcquireOperator) {
				acquireCount = t1WireFragToSensors(
					acquireCount, currentSite, config, fragComp, op);

			} else if (op instanceof SensornetDeliverOperator) {
				String sendInterface =
					CodeGenUtils.generateProviderSendInterfaceName(
					"AM_DELIVERMESSAGE");
				config.addWiring(fragComp.getID(), COMPONENT_RADIO,
					INTERFACE_SEND, "SendDeliver", sendInterface);
			}
		}
		return acquireCount;
	}


	/**
	 * TOS1: Wire fragment component to the sensor component.
	 * @param acquireCount
	 * @param currentSite
	 * @param config
	 * @param fragComp
	 * @param op
	 * @return
	 * @throws CodeGenerationException
	 */
	private int t1WireFragToSensors(int acquireCount, final Site currentSite,
			final NesCConfiguration config, final FragmentComponent fragComp,
			final SensornetOperator op) throws CodeGenerationException {

		final SensorT1Component sensorComp = new SensorT1Component(
				currentSite, currentSite, COMPONENT_SENSOR, config,
				tossimFlag);

		config.addComponent(sensorComp);

		final int numSensedAttr
			= ((AcquireOperator) op.getLogicalOperator()).getNumSensedAttributes();
		for (int i = 0; i < numSensedAttr; i++) {
			acquireCount++;

			if (!tossimFlag) {
				config.addWiring(fragComp.getID(),
					COMPONENT_SENSOR, INTERFACE_READ,
					"Op" + op.getID() + INTERFACE_READ
					+ i,
					INTERFACE_SENSOR);
			} else {
				config.addWiring(fragComp.getID(),
					COMPONENT_SENSOR, INTERFACE_READ,
					"Op" + op.getID() + INTERFACE_READ + i,
					"ADC[" + acquireCount+ "]");
			}
		}
		return acquireCount;
	}


	/**
	 * TOS1: Wire the fragments with trays, radio receive and transmit.
	 * @param currentSite The site for which code is being generated.
	 * @param currentSiteID The id of the site for which code is being generated.
	 * @param config The nesC configuration which components/wirings are being added to.
	 * @throws CodeGenerationException
	 */
    private void wireSiteFragments(final Site currentSite, final String currentSiteID,
			final NesCConfiguration config) throws CodeGenerationException {

		/* Wire fragments up accordingly */
		final Iterator<ExchangePart> exchPartIter =
			currentSite.getExchangeComponents().iterator();
		while (exchPartIter.hasNext()) {
			final ExchangePart exchPart = exchPartIter.next();
			final Fragment sourceFrag = exchPart.getSourceFrag();
			final Fragment destFrag = exchPart.getDestFrag();
			final String destSiteID = exchPart.getDestSiteID();

			final String trayName = addTrayComponent(currentSite, config, sourceFrag, destFrag,
					destSiteID);

			/* linking two remote fragments */
			if (exchPart.isRemote()) {
				
				/* Remote consumer */
				if (exchPart.getComponentType()
						== ExchangePartType.CONSUMER) {
					addRemoteConsumer(currentSite, currentSiteID, config, exchPart,
							sourceFrag, destFrag, destSiteID, trayName);

				/* Remote producer */
				} else if (exchPart.getComponentType()
						== ExchangePartType.PRODUCER) {
					addRemoteProducer(currentSite, currentSiteID, config, exchPart,
							sourceFrag, destFrag, destSiteID, trayName);

				/* Relay */
				} else if (exchPart.getComponentType()
						== ExchangePartType.RELAY) {
					addRelay(currentSite,
							currentSiteID, config, exchPart, sourceFrag,
							destFrag, destSiteID, trayName);
				}

			} else {
				/* link two local fragments on the same site */
				connectLocalFragments(currentSite, config, sourceFrag, 
						destFrag, trayName);
			}
		}
	}


    /**
     * Add a remote consumer.
     * @param currentSite
     * @param currentSiteID
     * @param config
     * @param exchComp
     * @param sourceFrag
     * @param destFrag
     * @param destSiteID
     * @param trayName
     * @throws CodeGenerationException
     */
	private void addRemoteConsumer(final Site currentSite, final String currentSiteID,
			final NesCConfiguration config, final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {
		final String destFragCompName = FragmentComponent
			.generateName(destFrag, currentSite, tosVersion);
		config.addWiring(destFragCompName,
			trayName,
			CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(sourceFrag),
			CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(sourceFrag),
			INTERFACE_GET_TUPLES);

		if (tosVersion==1) {
			t1AddRXComponent(currentSite, currentSiteID,
				config, exchPart, sourceFrag, destFrag,
				destSiteID, trayName);
		} else {
			t2AddRXComponent(config, exchPart,
					sourceFrag, destFrag, destSiteID, trayName);
		}
	}


	/**
	 * TOS1: Add a remote producer.
	 * @param plan
	 * @param qos
	 * @param tosVersion
	 * @param tossimFlag
	 * @param currentSite
	 * @param currentSiteID
	 * @param config
	 * @param exchComp
	 * @param sourceFrag
	 * @param destFrag
	 * @param destSiteID
	 * @param trayName
	 * @throws CodeGenerationException
	 */
	private void addRemoteProducer(final Site currentSite, final String currentSiteID,
			final NesCConfiguration config, final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {
		final String sourceFragCompName = FragmentComponent
			.generateName(sourceFrag, currentSite, tosVersion);
		config.addWiring(sourceFragCompName,
			trayName,
			CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(sourceFrag),
			CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(sourceFrag),
			INTERFACE_PUT_TUPLES);

		if (tosVersion==1) {
			t1AddTXComponent(currentSiteID, config,
					exchPart, sourceFrag, destFrag, destSiteID,
					trayName);			
		} else {
			t2AddTXComponent(config, exchPart, sourceFrag,
					destFrag, destSiteID, trayName);
		}
	}


	/**
	 * TOS1: Add relay.
	 * @param currentSite
	 * @param currentSiteID
	 * @param config
	 * @param exchComp
	 * @param sourceFrag
	 * @param destFrag
	 * @param destSiteID
	 * @param trayName
	 * @throws CodeGenerationException
	 */
	private void addRelay(final Site currentSite,
			final String currentSiteID, final NesCConfiguration config,
			final ExchangePart exchPart, final Fragment sourceFrag,
			final Fragment destFrag, final String destSiteID,
			final String trayName) throws CodeGenerationException {
		
		if (tosVersion==1) {
			t1AddRXComponent(currentSite, currentSiteID,
				config, exchPart, sourceFrag, destFrag,
				destSiteID, trayName);
			t1AddTXComponent(currentSiteID, config,
				exchPart, sourceFrag, destFrag, destSiteID,
				trayName);
		} else {
			t2AddRXComponent(config, exchPart, sourceFrag, destFrag, 
					destSiteID, trayName);
			t2AddTXComponent(config, exchPart, sourceFrag, destFrag, 
					destSiteID, trayName);
		}
	}


	/**
	 * TOS1: Connect two local fragments via a tray.
	 * @param tosVersion
	 * @param currentSite
	 * @param config
	 * @param sourceFrag
	 * @param destFrag
	 * @param trayName
	 * @throws CodeGenerationException
	 */
	private void connectLocalFragments(final Site currentSite, 
			final NesCConfiguration config,
			final Fragment sourceFrag, final Fragment destFrag,
			final String trayName) throws CodeGenerationException {
		final String destFragCompName = FragmentComponent
			.generateName(destFrag, currentSite, tosVersion);
		config.addWiring(
				destFragCompName,
				trayName,
				CodeGenUtils
					.generateGetTuplesInterfaceInstanceName(sourceFrag),
				CodeGenUtils
					.generateGetTuplesInterfaceInstanceName(sourceFrag),
				INTERFACE_GET_TUPLES);

		final String sourceFragCompName = FragmentComponent
			.generateName(sourceFrag, currentSite, tosVersion);
		config.addWiring(sourceFragCompName, trayName,
				CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
				CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
				INTERFACE_PUT_TUPLES);
	}


	/**
	 * TOS1: Creates a tray to buffer tuples for an exachange part
	 * (producer, consumer or relay).
	 * @param config
	 * @param sourceFrag
	 * @param destFrag
	 * @param destSiteID
	 * @return
	 * @throws CodeGenerationException
	 */
	private String addTrayComponent(final Site currentSite,
			final NesCConfiguration config, final Fragment sourceFrag,
			final Fragment destFrag, final String destSiteID)
			throws CodeGenerationException {
		TrayComponent trayComp = new TrayComponent(sourceFrag, destFrag, 
				destSiteID, currentSite, config, plan, tosVersion, 
				tossimFlag, costParams, debugLeds);
		
		trayComp = (TrayComponent) config.addComponent(trayComp);
		// tray may already exist
		final String trayName = trayComp.getID();
		return trayName;
	}


    /**
     * TOS1: Adds a tray-TX component wiring to a configuration.  The TX
     * component is added if not already present.
     * @param plan The query plan which code is being generated for.
     * @param qos The user-specified quality-of-service requirements.
     * @param currentSiteID The id of the site for which code is being generated.
     * @param config The nesC configuration which components/wirings are being added to.
     * @param exchComp The corresponding exchange producer.
     * @param sourceFrag The source Fragment (i.e., tuple type) of tuples to be transmitted.
     * @param destFrag The destination Fragment of tuples to be transmitted.
     * @param destSiteID The destination site of tuples to be transmitted.
     * @param trayName The name of the tray from which tuples are to be read from.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @throws CodeGenerationException
     */
    private void t1AddTXComponent(final String currentSiteID,
    	    final NesCConfiguration config, final ExchangePart exchComp,
    	    final Fragment sourceFrag, final Fragment destFrag,
    	    final String destSiteID, final String trayName)
    	    throws CodeGenerationException {

    	TXT1Component txComp = new TXT1Component(exchComp.getCurrentSite(),
    		exchComp.getNext().getCurrentSite(), config, plan, tossimFlag, 
    		costParams, debugLeds);
    	txComp = (TXT1Component) config.addComponent(txComp);
    	txComp.addExchangeComponent(exchComp);
    	final String txCompName = txComp.getID();

    	config.addWiring(txCompName, trayName, CodeGenUtils
    		.generateGetTuplesInterfaceInstanceName(sourceFrag),
    		CodeGenUtils.generateGetTuplesInterfaceInstanceName(sourceFrag,
    			destFrag, destSiteID, currentSiteID),
    		INTERFACE_GET_TUPLES);
    	config.addWiring(txCompName, COMPONENT_RADIO, INTERFACE_SEND,
    		CodeGenUtils.generateUserSendInterfaceName(sourceFrag,
    			destFrag, destSiteID), CodeGenUtils
    			.generateProviderSendInterfaceName(sourceFrag,
    				destFrag, destSiteID, currentSiteID));

    	// Original Timer code
    	config.addWiring(COMPONENT_QUERY_PLAN, txCompName, INTERFACE_DO_TASK,
    		INTERFACE_DO_TASK + txCompName, INTERFACE_DO_TASK);
    }


    /**
     * TOS1: Adds a RX-tray component wiring to a configuration.  The RX
     * component is added if not already present.
     * @param currentSite The site for which code is being generated.
     * @param currentSiteID The site id for which code is being generated.
     * @param config The nesC configuration which components/wirings are being added to.
     * @param exchComp The corresponding exchange consumer.
     * @param sourceFrag The source Fragment (i.e., tuple type) of tuples to be transmitted.
     * @param destFrag The destination Fragment of tuples to be transmitted.
     * @param destSiteID The destination site of tuples to be transmitted.
     * @param trayName The name of the tray from which tuples are to be written to.
     * @throws CodeGenerationException
     */
    private void t1AddRXComponent(final Site currentSite,
    	    final String currentSiteID, final NesCConfiguration config,
    	    final ExchangePart exchComp, final Fragment sourceFrag,
    	    final Fragment destFrag, final String destSiteID,
    	    final String trayName) throws CodeGenerationException {
    	RXT1Component rxComp = new RXT1Component(exchComp.getPrevious()
    		.getCurrentSite(), currentSite, config, plan, tossimFlag, debugLeds);
    	rxComp = (RXT1Component) config.addComponent(rxComp); // may already
    	// exist
    	rxComp.addExchangeComponent(exchComp);
    	final String rxCompName = rxComp.getID();

    	config
    		.addWiring(rxCompName, trayName,
    			CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
    			CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
    			INTERFACE_PUT_TUPLES);
    	config.addWiring(rxCompName, COMPONENT_RADIO, INTERFACE_RECEIVE,
    		CodeGenUtils.generateUserReceiveInterfaceName(sourceFrag,
    			destFrag, destSiteID), CodeGenUtils
    			.generateProviderReceiveInterfaceName(sourceFrag,
    				destFrag, destSiteID, exchComp.getPrevious()
    					.getCurrentSiteID()));
    	config.addWiring(COMPONENT_QUERY_PLAN, rxCompName, INTERFACE_DO_TASK,
    		INTERFACE_DO_TASK + rxCompName, INTERFACE_DO_TASK);
        }


    /**
     * TOS2: Generates the top-level configurations for each site in the sensor
     * network.
     * @param plan The query plan which code is being generated for.
     * @param qos The user-specified quality-of-service requirements.
     * @param sink The sink node of the network.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @return
     * @throws IOException
     * @throws CodeGenerationException
     */
    private HashMap<Site, NesCConfiguration> t2GenerateSiteConfigs()
	    throws IOException, CodeGenerationException {

		final HashMap<Site, NesCConfiguration> nodeConfigs = new HashMap<Site, NesCConfiguration>();

		final Iterator<Site> siteIter = plan.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			final Site currentSite = siteIter.next();
			final String currentSiteID = currentSite.getID();

			/* Instantiate the top-level site configuration */
			final NesCConfiguration config = new NesCConfiguration(
				COMPONENT_QUERY_PLANC + currentSiteID, //$
				plan, currentSite, tosVersion, tossimFlag);

			/* Add the components which are always needed */
			t2AddMainSiteComponents(config);

			/* Optional components for Led debugging not implemented yet for TOS2 */

			/* Power Management components not implemented yet for TOS2*/

			/* Functionality for changing radio transmit power not implemented yet for TOS2*/

			/* Add the fragments which have been placed onto this site */
			t2AddSiteFragments(currentSite, config);

			/* Wire the fragments with trays, radio receive and transmit */
			wireSiteFragments(currentSite, currentSiteID, config);

			nodeConfigs.put(currentSite, config);

		}
		return nodeConfigs;
    }


    /**
     * TOS2: Add the components which are always needed.
     * @param plan The query plan which code is being generated for.
     * @param qos The user-specified quality-of-service requirements.
     * @param sink The sink node of the network.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @param config The nesC configuration which components/wirings are being added to.
     * @throws CodeGenerationException
     */
	private void t2AddMainSiteComponents(final NesCConfiguration config) throws CodeGenerationException {
		MainComponent mainComp = new MainComponent(COMPONENT_MAIN, config,
			tosVersion, tossimFlag);
		config.addComponent(mainComp);

		int sink = plan.getGateway();
		QueryPlanModuleComponent queryPlanModuleComp =
			new QueryPlanModuleComponent(COMPONENT_QUERY_PLAN, config,
			plan, sink, tosVersion, (tossimFlag || combinedImage), targetName,
			costParams, controlRadioOff, enablePrintf, useStartUpProtocol, enableLeds,
			debugLeds, usePowerManagement, deliverLast, adjustRadioPower);
		config.addComponent(queryPlanModuleComp);

		TimerT2Component timerComp = new TimerT2Component(		//$
			COMPONENT_AGENDA_TIMER, config, tossimFlag);
		config.addComponent(timerComp);

		//Radio component and serial component are separate in T2
		final RadioComponent radioComp = new RadioComponent(
			COMPONENT_RADIO, config, tossimFlag);
		config.addComponent(radioComp);
		//Required to turn the radio on/off
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_RADIO, INTERFACE_SPLITCONTROL, "CommControl",
				"SplitControl");
		
		//Entry point to the query plan code
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_MAIN,
			INTERFACE_BOOT, INTERFACE_BOOT, INTERFACE_BOOT);

		//Used to trigger agenda tasks
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_AGENDA_TIMER,
			INTERFACE_TIMER, TYPE_TMILLI, COMPONENT_AGENDA_TIMER,
			INTERFACE_TIMER);
//		if (!tossimFlag) {
//			TimerT2Component radioOnComp = new TimerT2Component(		//$
//					"RadioOnTimer", config, tossimFlag);
//			config.addComponent(radioOnComp);
//			config.addWiring(COMPONENT_QUERY_PLAN, "RadioOnTimer",
//					INTERFACE_TIMER, TYPE_TMILLI, "RadioOnTimer", INTERFACE_TIMER);
//		}
		
		if (this.includeDeluge && tossimFlag == false) {
			DelugeComponent delugeComp = new DelugeComponent(COMPONENT_DELUGE, config, tossimFlag);
			config.addComponent(delugeComp);
		}
		if (this.useStartUpProtocol) {
			
			SerialComponent serialComp = new SerialComponent(COMPONENT_SERIAL_DEVICE, config, tossimFlag);
			config.addComponent(serialComp);
			config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_SERIAL_DEVICE, INTERFACE_SPLITCONTROL, "SerialControl", INTERFACE_SPLITCONTROL);
			
			String aid = ActiveMessageIDGenerator.getActiveMessageID("AM_SERIAL_STARTUP_MESSAGE");
			SerialAMReceiveComponent serialRxComp = 
				new SerialAMReceiveComponent(COMPONENT_SERIALRX+"StartUp", config, aid, tossimFlag);
			config.addComponent(serialRxComp);
			config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_SERIALRX+"StartUp", INTERFACE_RECEIVE, "SerialStartUp", "Receive");
			
			aid = ActiveMessageIDGenerator.getActiveMessageID("AM_BEACON_MESSAGE");
			AMSendComponent beaconSender = new AMSendComponent(config, aid, tossimFlag);
			config.addComponent(beaconSender);
			config.addWiring(COMPONENT_QUERY_PLAN, beaconSender.getID(),
					INTERFACE_SEND, "Beacon"+INTERFACE_SEND, INTERFACE_SEND);
			config.addWiring(COMPONENT_QUERY_PLAN, beaconSender.getID(),
					INTERFACE_PACKET, "Beacon"+INTERFACE_PACKET,
					INTERFACE_PACKET);
			config.addWiring(COMPONENT_QUERY_PLAN, beaconSender.getID(),
						INTERFACE_AMPACKET, "Beacon"+INTERFACE_AMPACKET,
						INTERFACE_AMPACKET);
			
			AMRecieveComponent beaconReceiver = new AMRecieveComponent(config, aid, tossimFlag);
			config.addComponent(beaconReceiver);
			config.addWiring(COMPONENT_QUERY_PLAN, beaconReceiver.getID(),
					INTERFACE_RECEIVE, "Beacon"+INTERFACE_RECEIVE,
					INTERFACE_RECEIVE);

		}
	}


    /**
     * TOS2: Add the fragments which have been placed onto this site.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @param currentSite The site for which code is being generated.
     * @param config The nesC configuration which components/wirings are being added to.
     * @throws CodeGenerationException
     */
	private void t2AddSiteFragments(final Site currentSite, final NesCConfiguration config)
			throws CodeGenerationException {
		final Iterator<Fragment> fragIter = currentSite.getFragments()
			.iterator();
		while (fragIter.hasNext()) {
			final Fragment frag = fragIter.next();

			/* Add component for current fragment */
			final FragmentComponent fragComp = new FragmentComponent(
				frag, config, tosVersion, tossimFlag);
			config.addComponent(fragComp);

			/* Wire fragment to main query plan component */
			config.addWiring(COMPONENT_QUERY_PLAN, fragComp.getID(),
				INTERFACE_DO_TASK, CodeGenUtils
					.generateUserAsDoTaskName(frag, currentSite),
				INTERFACE_DO_TASK);

			/* Wire fragment to hardware devices as required */
			t2WireFragToDevices(currentSite, config, frag, fragComp);
		}
	}


	/**
	 * TOS2: Wire fragment to hardware devices as required
	 * @param tossimFlag
	 * @param currentSite
	 * @param config
	 * @param frag
	 * @param fragComp
	 * @throws CodeGenerationException
	 */
	private void t2WireFragToDevices(final Site currentSite, 
	final NesCConfiguration config, final Fragment frag, final FragmentComponent fragComp)
	throws CodeGenerationException {
		final Iterator<SensornetOperator> opIter = frag
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
			final SensornetOperator op = opIter.next();
			if (op instanceof SensornetAcquireOperator) {

				/* Wire fragment component to the sensor component */
				t2wireFragToSensors(currentSite, config, fragComp, op);
				
				if (this.showLocalTime) {
					t2wireFragToLocalTime(currentSite, config, fragComp, op);
				}
				
			} else if (op instanceof SensornetDeliverOperator) {
				
				/* Wire fragment component to serial device */
				SerialComponent serialComp = new SerialComponent(COMPONENT_SERIAL_DEVICE, config, tossimFlag);
				config.addComponent(serialComp);
				config.addWiring(fragComp.getID(), serialComp.getID(), INTERFACE_SPLITCONTROL, 
						"SerialAMControl", INTERFACE_SPLITCONTROL);

				SerialAMSendComponent serialSendComp = new SerialAMSendComponent(currentSite,frag,null,null,
						COMPONENT_SERIALTX, config, "AM_DELIVERMESSAGE", tossimFlag);
				config.addComponent(serialSendComp);
				
				ActiveMessageIDGenerator.getActiveMessageID("AM_DELIVERMESSAGE");
				
				config.addWiring(fragComp.getID(), serialSendComp.getID(),
					INTERFACE_SEND, "SendDeliver", INTERFACE_SEND);
				config.addWiring(fragComp.getID(), serialSendComp.getID(),
						INTERFACE_PACKET);
			}
		}
	}


	/**
	 * TOS2: Wire fragment component to the sensor component.
	 * @param tossimFlag
	 * @param currentSite
	 * @param config
	 * @param fragComp
	 * @param op
	 * @throws CodeGenerationException
	 */
	private void t2wireFragToSensors(final Site currentSite, 
	final NesCConfiguration config, final FragmentComponent fragComp, final SensornetOperator op)
	throws CodeGenerationException {
		final int numSensedAttr
		= ((AcquireOperator) op.getLogicalOperator()).getNumSensedAttributes();
		for (int i = 0; i < numSensedAttr; i++) {
			//TODO: look up sensorID in metadata
			String sensorId = new Integer(i).toString();
			final SensorT2Component sensorComp = new SensorT2Component(
					currentSite, sensorId, COMPONENT_SENSOR, config, "",
					tossimFlag);
			config.addComponent(sensorComp);
			final String sensorName = sensorComp.getID();

			config.addWiring(fragComp.getID(), sensorName,
					INTERFACE_READ, TYPE_READ,
					"Op" + op.getID() + INTERFACE_READ + i, INTERFACE_READ);
		}
	}

	private void t2wireFragToLocalTime(final Site currentSite, 
	final NesCConfiguration config, final FragmentComponent fragComp, final SensornetOperator op)
	throws CodeGenerationException {

		final LocalTimeComponent localTimeComp = new LocalTimeComponent(
					COMPONENT_LOCAL_TIME, config, tossimFlag);
			config.addComponent(localTimeComp);

			config.addWiring(fragComp.getID(), COMPONENT_LOCAL_TIME,
					INTERFACE_LOCAL_TIME, TYPE_TMILLI, INTERFACE_LOCAL_TIME, INTERFACE_LOCAL_TIME);
	}

	private void t2AddTXComponent(final NesCConfiguration config, final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {
		
		final String txActiveMessageIDKey =
			ActiveMessageIDGenerator.getActiveMessageIDKey(
					sourceFrag.getID(),
					destFrag.getID(),
					destSiteID,
					exchPart.getCurrentSite().getID());

		final TXT2Component txComp = new TXT2Component(
				sourceFrag, destFrag,
				exchPart.getDestSite(),
				exchPart.getNext().getCurrentSite(),
				config, plan, tossimFlag, costParams, debugLeds);

		config.addComponent(txComp);
		final String txCompName = txComp.getID();

		final AMSendComponent radioTxComp = new AMSendComponent(
			exchPart.getCurrentSite(), sourceFrag, exchPart
				.getNext().getCurrentSite(), destFrag,
			COMPONENT_RADIOTX, config, txActiveMessageIDKey,
			tossimFlag);
		config.addComponent(radioTxComp);

		config.addWiring(txCompName, trayName,
				CodeGenUtils.generateGetTuplesInterfaceInstanceName(sourceFrag),
				INTERFACE_GET_TUPLES, INTERFACE_GET_TUPLES);
		config.addWiring(txCompName, radioTxComp.getID(),
			INTERFACE_SEND, INTERFACE_SEND, INTERFACE_SEND);

		config.addWiring(txCompName, radioTxComp.getID(),
			INTERFACE_PACKET, INTERFACE_PACKET, INTERFACE_PACKET);

		config.addWiring(txCompName, radioTxComp.getID(),
				INTERFACE_AMPACKET, INTERFACE_AMPACKET, INTERFACE_AMPACKET);
		
		config.addWiring(COMPONENT_QUERY_PLAN, txCompName,
			INTERFACE_DO_TASK, CodeGenUtils.generateUserAsDoTaskName("tx",
					exchPart.getCurrentSite().getID(), sourceFrag,
					exchPart.getNext().getCurrentSite().getID(), destFrag),
					INTERFACE_DO_TASK);
	}

	
	private void t2AddRXComponent(final NesCConfiguration config, 
			final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {

		final RXT2Component rxComp = new RXT2Component(
			sourceFrag, destFrag,
			exchPart.getDestSite(),
			exchPart.getPrevious().getCurrentSite(),
			config, plan, tossimFlag, debugLeds);
		config.addComponent(rxComp);
		final String rxCompName = rxComp.getID();

		final String rxActiveMessageIDKey =
			ActiveMessageIDGenerator.getActiveMessageIDKey(
				sourceFrag.getID(),
				destFrag.getID(),
				destSiteID,
				exchPart.getPrevious().getCurrentSite().getID());
		// exchComp.getPrevious().getCurrentNode().getID(),
		// exchComp.getCurrentNode().getID());

		final AMRecieveComponent radioRxComp = new AMRecieveComponent(
			sourceFrag,
			exchPart.getDestSite(),
			destFrag,
			exchPart.getPrevious().getCurrentSite(),
			COMPONENT_RADIORX,
			config,
			rxActiveMessageIDKey,
			tossimFlag);
		config.addComponent(radioRxComp);

		config.addWiring(rxCompName,
			trayName,
			CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(sourceFrag),
			INTERFACE_PUT_TUPLES,
			INTERFACE_PUT_TUPLES);
		config.addWiring(rxCompName, radioRxComp.getID(),
			INTERFACE_RECEIVE, INTERFACE_RECEIVE,
			INTERFACE_RECEIVE);

		config.addWiring(COMPONENT_QUERY_PLAN, rxCompName,
			INTERFACE_DO_TASK, CodeGenUtils
				.generateUserAsDoTaskName("rx",
					exchPart.getPrevious()
						.getCurrentSite()
						.getID(), sourceFrag,
					exchPart.getCurrentSite()
						.getID(), destFrag),
			INTERFACE_DO_TASK);
	}


    /**
     * Generate the configuration (i.e., wiring) with components for all
     * sites, used by Tossim.  This done by merging all the site
     * configurations into a single configuration.  This is necessary because
     * Tossim has the limitation that every site executes the same
     * query plan.
     *
     * @param plan The query plan which code is being generated for.
     * @param siteConfigs The collection of individual site configurations.
     * @return
     * @throws IOException
     */
    private NesCConfiguration generateCombinedConfiguration(
    final HashMap<Site, NesCConfiguration> siteConfigs)
    throws IOException {

		final NesCConfiguration tossimConfiguration = new NesCConfiguration(
			COMPONENT_QUERY_PLANC, plan, tosVersion, tossimFlag);

		final Iterator<Site> siteIter = siteConfigs.keySet().iterator();
		while (siteIter.hasNext()) {
		    final Site currentSite = siteIter.next();
		    final NesCConfiguration configuration = siteConfigs
			    .get(currentSite);
		    tossimConfiguration.mergeGraphs(configuration);
		}

		return tossimConfiguration;
    }


    /**
     * Factory method to instantiate an operator component
     * @param op The operator for which a component is being instantiated.
     * @param site The site that the operator instance has been placed on.
     * @param frag The fragment which the operator belongs to.
     * @param plan
     * @param qos
     * @param config
     * @param tosVersion
     * @param tossimFlag
     * @return
     * @throws CodeGenerationException
     */
    private NesCComponent instantiateOperatorNesCComponent(
	    final SensornetOperator op, final Site site,
	    final NesCConfiguration config) throws CodeGenerationException {

		if (op instanceof SensornetAcquireOperator) {
		    return new AcquireComponent((SensornetAcquireOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetAggrEvalOperator) {
		    return new AggrEvalComponent((SensornetAggrEvalOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetAggrInitOperator) {
		    return new AggrInitComponent((SensornetAggrInitOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetAggrMergeOperator) {
		    return new AggrMergeComponent((SensornetAggrMergeOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetDeliverOperator) {
		    return new DeliverComponent((SensornetDeliverOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds, costParams);
		} else if (op instanceof SensornetExchangeOperator) {
		    return new ExchangeProducerComponent((SensornetExchangeOperator) op, plan,
			    config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetNestedLoopJoinOperator) {
		    return new JoinComponent((SensornetNestedLoopJoinOperator) op, plan, config,
		    		tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetProjectOperator) {
		    return new ProjectComponent((SensornetProjectOperator) op, plan,
		    		config, tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetSelectOperator) {
		    return new SelectComponent((SensornetSelectOperator) op, plan, config,
		    		tosVersion, tossimFlag, debugLeds);
		} else if (op instanceof SensornetWindowOperator) {
		    return new WindowComponent((SensornetWindowOperator) op, plan, config,
		    		tosVersion, tossimFlag, debugLeds);
		} else {
		    throw new CodeGenerationException(
			    "No NesC Component found for operator type="
				    + op.getClass().toString() + ", id=" + op.getID());
		}
    }


    /**
     * Generate operator configurations for each instance of each fragment.
     * @param nodeConfigs
     * @throws IOException
     * @throws CodeGenerationException
     */
    private HashMap<String, NesCConfiguration> generateFragmentConfigurations(
	    final HashMap<Site, NesCConfiguration> nodeConfigs) throws IOException, CodeGenerationException {

		// For each fragment on each node, generate finer-grained configuration
		// (i.e., operator-level)

		final HashMap<String, NesCConfiguration> fragConfigs = new HashMap<String, NesCConfiguration>();

		// for each site in the sensor network
		final Iterator<Site> siteIter = plan.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
		    final Site site = siteIter.next();

		    // for each fragment on the node
		    final Iterator<Fragment> fragIter = site.getFragments().iterator();
		    while (fragIter.hasNext()) {
				final Fragment frag = fragIter.next();

				final String fragName = FragmentComponent.generateName(frag,
					site, tosVersion);
				final FragmentComponent fragComp = (FragmentComponent)
					nodeConfigs.get(site).getComponent(fragName);
				final NesCConfiguration fragConfig = new NesCConfiguration(
					fragName, plan, site, tosVersion, tossimFlag);
				fragComp.setInnerConfig(fragConfig);

				generateIntraFragmentConfig(site, frag, fragConfig);
				fragConfigs.put(fragName, fragConfig);
		    }//while (fragIter.hasNext()) {

		}//	while (siteIter.hasNext()) {


		return fragConfigs;
    }


    /**
     * Generates operator configuration for a fragment instance.
     * @param plan
     * @param qos
     * @param tosVersion
     * @param tossimFlag
     * @param site
     * @param frag
     * @param fragConfig
     * @throws CodeGenerationException
     */
	private void generateIntraFragmentConfig(final Site site, final Fragment frag,
			final NesCConfiguration fragConfig) throws CodeGenerationException {

		/* Wire the operators inside the fragment to each other */
		addAndWireOperators(site, frag,	fragConfig);

		/* add the producer and link to outside world*/
		final SensornetOperator rootOp = frag.getRootOperator();
		final String fragID = frag.getID();
		final String rootOpName = CodeGenUtils
			.generateOperatorInstanceName(rootOp, site, tosVersion);
		final SensornetExchangeOperator producerOp = (SensornetExchangeOperator) frag
			.getParentExchangeOperator();
		if (producerOp != null) {
		    addExternalProducerWiring(site, frag,
					fragConfig, rootOp, fragID, rootOpName, producerOp);

		} else {
			/* Wire the deliver operator to the outside world */
		    addExternalDeliverWiring(fragConfig, rootOpName);
		}

		/* Wire the acquisition operators to the outside world */
		addExternalSensorWiring(site, frag, fragConfig);
		
		if (this.showLocalTime && tosVersion==2) {
			addExternalLocalTimeWiring(site, frag, fragConfig);
		}
	}


	/**
	 * Creates the operators in the fragment except the producer and
	 * wires them to each other.
	 * @param site
	 * @param frag
	 * @param fragConfig
	 * @throws CodeGenerationException
	 */
	private void addAndWireOperators(final Site site, final Fragment frag,
			final NesCConfiguration fragConfig) throws CodeGenerationException {
		// for each operator in the fragment
		Iterator<SensornetOperator> opIter = frag
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = opIter.next();
		    final NesCComponent opComp = instantiateOperatorNesCComponent(
			    op, site, fragConfig);
		    final String opName = opComp.getID();
		    fragConfig.addComponent(opComp);
		    opComp.setDescription(op.toString());

		    // link to each child of that operator
		    final Iterator<SensornetOperator> childOpIter = op
			    .childOperatorIterator();
		    int childCount = 0;
		    while (childOpIter.hasNext()) {
				final SensornetOperator childOp = childOpIter.next();
				final String childOpName = CodeGenUtils
					.generateOperatorInstanceName(childOp, site, tosVersion);
				final String requestDataInterfaceType = CodeGenUtils
					.generateGetTuplesInterfaceInstanceName(childOp);

				if (childOp instanceof SensornetExchangeOperator) {
				    addExternalTrayWiring(fragConfig, op, opName, childCount,
							childOp, requestDataInterfaceType);
				} else {
				    addIntraFragWiring(fragConfig, op, opName, childCount,
							childOpName, requestDataInterfaceType);
				}

				childCount++;
		    }
		}
	}


	/**
	 * Add wiring between two operators.
	 * @param fragConfig
	 * @param op
	 * @param opName
	 * @param childCount
	 * @param childOpName
	 * @param requestDataInterfaceType
	 * @throws CodeGenerationException
	 */
	private static void addIntraFragWiring(
			final NesCConfiguration fragConfig, final SensornetOperator op,
			final String opName, int childCount, final String childOpName,
			final String requestDataInterfaceType)
			throws CodeGenerationException {
		if (op.getInDegree() == 1) {
			fragConfig.addWiring(opName, childOpName,
				requestDataInterfaceType, "Child",
				"Parent");
		} else if ((op.getInDegree() == 2)
		    && (childCount == 0)) {
			fragConfig.addWiring(opName, childOpName,
				requestDataInterfaceType, "LeftChild",
				"Parent");
		} else if ((op.getInDegree() == 2)
		    && (childCount == 1)) {
			fragConfig.addWiring(opName, childOpName,
				requestDataInterfaceType, "RightChild",
				"Parent");
		}
	}


	/**
	 * Add external wiring between an operator which receive tuples from
	 * operators in other fragments and a tray.
	 * @param fragConfig
	 * @param op
	 * @param opName
	 * @param childCount
	 * @param childOp
	 * @param requestDataInterfaceType
	 * @throws CodeGenerationException
	 */
	private void addExternalTrayWiring(
			final NesCConfiguration fragConfig, final SensornetOperator op,
			final String opName, int childCount, final SensornetOperator childOp,
			final String requestDataInterfaceType)
			throws CodeGenerationException {
		// this is a "leaf" operator of the fragment, so
		// needs to read data from input tray(s)
		// String trayGetName =
		// INTERFACE_TRAY_GET+"Frag"+((LogicalExchangeOperator)childOp).getChildFragment().getFragID();
		final String trayGetName = CodeGenUtils
		    .generateGetTuplesInterfaceInstanceName(((SensornetExchangeOperator) childOp)
			    .getSourceFragment());

		if (op.getInDegree() == 1) {
			fragConfig.linkToExternalProvider(opName,
				requestDataInterfaceType, "Child",
				trayGetName);
		    } else if ((op.getInDegree() == 2)
			    && (childCount == 0)) {
			fragConfig.linkToExternalProvider(opName,
				requestDataInterfaceType, "LeftChild",
				trayGetName);
		    } else if ((op.getInDegree() == 2)
			    && (childCount == 1)) {
			fragConfig.linkToExternalProvider(opName,
				requestDataInterfaceType, "RightChild",
				trayGetName);
		 }
	}


	/**
	 * Add external wiring between producer operator component and tray
	 * component that it forwards tuples to.
	 * @param plan
	 * @param qos
	 * @param tosVersion
	 * @param tossimFlag
	 * @param site
	 * @param frag
	 * @param fragConfig
	 * @param rootOp
	 * @param fragID
	 * @param rootOpName
	 * @param producerOp
	 * @throws CodeGenerationException
	 */
	private void addExternalProducerWiring(final Site site, final Fragment frag,
			final NesCConfiguration fragConfig, final SensornetOperator rootOp,
			final String fragID, final String rootOpName,
			final SensornetExchangeOperator producerOp) throws CodeGenerationException {
		final ExchangeProducerComponent producerComp = new ExchangeProducerComponent(
		    producerOp, plan, fragConfig, tosVersion, tossimFlag, debugLeds);
		fragConfig.addComponent(producerComp);
		final String producerOpID = producerComp.getID();
		fragConfig.addWiring(producerOpID, rootOpName, CodeGenUtils
		    .generateGetTuplesInterfaceInstanceName(rootOp),
		    "Child", "Parent");
		fragConfig.linkToExternalUser(producerOpID,
		    INTERFACE_DO_TASK, INTERFACE_DO_TASK,
		    INTERFACE_DO_TASK);
		producerComp.setDescription(producerOp.toString());

		final Iterator<ExchangePart> exchCompIter = site
		    .getExchangeComponents().iterator();
		while (exchCompIter.hasNext()) {
			final ExchangePart exchComp = exchCompIter.next();

			if (exchComp.getSourceFragID().equals(fragID)
				&& (exchComp.getComponentType() == ExchangePartType.PRODUCER)) {

			    fragConfig.linkToExternalProvider(producerOpID,
					CodeGenUtils.generatePutTuplesInterfaceInstanceName(frag),
					INTERFACE_PUT_TUPLES,
					CodeGenUtils.generatePutTuplesInterfaceInstanceName(frag));
			}
		}
	}


	/**
	 * Add external wiring between deliver operator component and component
	 * which receives messages for the serial port (radio in tinyOS 1)
	 * @param fragConfig
	 * @param rootOpName
	 * @throws CodeGenerationException
	 */
	private void addExternalDeliverWiring(
			final NesCConfiguration fragConfig, final String rootOpName)
			throws CodeGenerationException {
		// deliver operator
		fragConfig.linkToExternalUser(rootOpName,
		    INTERFACE_DO_TASK, INTERFACE_DO_TASK,
		    INTERFACE_DO_TASK);
		fragConfig.linkToExternalProvider(rootOpName, INTERFACE_SEND,
			"SendDeliver", "SendDeliver");
		
		if (tosVersion==2){
			fragConfig.linkToExternalProvider(rootOpName, INTERFACE_SPLITCONTROL, 
					"SerialAMControl", "SerialAMControl");
			fragConfig.linkToExternalProvider(rootOpName, INTERFACE_PACKET,
					"Packet", "Packet");
		}
	}


	/**
	 * Add external wiring between acquire operator components and
	 * sensor components.
	 * @param tosVersion
	 * @param site
	 * @param frag
	 * @param fragConfig
	 * @throws CodeGenerationException
	 */
	private void addExternalSensorWiring(final Site site, final Fragment frag,
			final NesCConfiguration fragConfig) throws CodeGenerationException {
		Iterator<SensornetOperator> opIter;
		// link any acquisition or scan operators to the outside world
		opIter = frag.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = opIter.next();
		    final String opName = CodeGenUtils
			    .generateOperatorInstanceName(op, site, tosVersion);
		    if (op instanceof SensornetAcquireOperator) {
			    final int numSensedAttr
			    	= ((AcquireOperator) op.getLogicalOperator()).getNumSensedAttributes();
			    for (int i = 0; i < numSensedAttr; i++) {
			    	if (tosVersion == 1) {
						fragConfig.linkToExternalProvider(opName,
							INTERFACE_READ,
							INTERFACE_READ + i,
							"Op" + op.getID() + INTERFACE_READ + i);
			    	} else {
					    fragConfig.linkToExternalProvider(opName,
						    INTERFACE_READ, TYPE_READ, INTERFACE_READ + i,
						    "Op" + op.getID() + INTERFACE_READ + i);
			    	}
			    }
		    }
		}
	}

	private void addExternalLocalTimeWiring(final Site site, final Fragment frag,
			final NesCConfiguration fragConfig) throws CodeGenerationException {
		Iterator<SensornetOperator> opIter;
		// link any acquisition or scan operators to the outside world
		opIter = frag.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = opIter.next();
		    final String opName = CodeGenUtils
			    .generateOperatorInstanceName(op, site, tosVersion);
		    if (op instanceof SensornetAcquireOperator) {
				fragConfig.linkToExternalProvider(opName,
						INTERFACE_LOCAL_TIME, TYPE_TMILLI,
						INTERFACE_LOCAL_TIME, INTERFACE_LOCAL_TIME);		    	
		    }
		}
	}
		
    /**
     * Creates and writes the nesC files with the code for the tossim
     * configuration, and the components within each configuration recursively.
     * @param plan The query plan which code is being generated for.
     * @param tossimConfig The single tossim configuration.
     * @param nescOutputDir The nesC output directory for generated code.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @throws IOException
     * @throws CodeGenerationException
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws URISyntaxException 
     */
    private void instantiateCombinedConfiguration(
    		final NesCConfiguration tossimConfig)
    		throws IOException, CodeGenerationException, 
    		SchemaMetadataException, TypeMappingException, OptimizationException, URISyntaxException{

		String outputDir = nescOutputDir + targetName+"/";
	    tossimConfig.instantiateComponents(outputDir);
	    //TODO: NesC configuration display
	    //tossimConfig.display(outputDir, tossimConfig.getName());
    }


    /**
     * Creates and writes the nesC files with the code for each site
     * configuration, and the components within each configuration recursively.
     * @param plan The query plan which code is being generated for.
     * @param siteConfigs The collection of individual site configurations.
     * @param nescOutputDir The nesC output directory for generated code.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @throws IOException
     * @throws CodeGenerationException
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws URISyntaxException 
     */
    private void instantiateSiteConfigurations(final HashMap<Site, NesCConfiguration> 
    siteConfigs) throws IOException, CodeGenerationException, SchemaMetadataException, 
    	TypeMappingException, OptimizationException, URISyntaxException {

	    final Iterator<Site> siteIter = plan
		    .siteIterator(TraversalOrder.POST_ORDER);
	    while (siteIter.hasNext()) {
			final Site currentSite = siteIter.next();
			final NesCConfiguration siteConfig = siteConfigs
				.get(currentSite);
			String outputDir = nescOutputDir + targetName+"/mote" + currentSite.getID() + "/";
			siteConfig.instantiateComponents(outputDir);
			//TODO: NesC configuration display
		    //siteConfig.display(outputDir, siteConfig.getName());
	    }
	}

    /**
     * TOS1/TOS2: Generates the QueryPlan.h header file.
     * @param plan The query plan which code is being generated for.
     * @param configs The collection of nesC configuration for each site.
     * @param nescOutputDir The nesC output directory for generated code.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @throws IOException
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    private void generateHeaderFiles(final HashMap<Site, NesCConfiguration> configs) 
    throws IOException, OptimizationException, SchemaMetadataException, TypeMappingException {

		final StringBuffer tupleTypeBuff = new StringBuffer();
		final StringBuffer messageTypeBuff = new StringBuffer();
		final StringBuffer activeIDDeclsBuff = new StringBuffer();

		addAMConstants(activeIDDeclsBuff);

		final Iterator<SensornetOperator> opIter = plan.getDAF().
			operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = (SensornetOperator) opIter.next();

		    /* Define the structures which define each operator or fragment
		     * output type. */
		    addOperatorTupleStructs(tupleTypeBuff, op);

		    /*Define the structures which define each message output
		     * type. */
		    addRadioMessageStructs(messageTypeBuff, op);
		}

		StringBuffer deliverMsgBuff;
		if (tosVersion == 1) {
			deliverMsgBuff = t1AddDeliverMessageStruct();			
		} else {
			deliverMsgBuff = t2AddDeliverMessageStruct(plan.getDAF().getRootOperator());
		}


		/* Generate combined (e.g., for Tossim or Deluge) header file */
		doGenerateCombinedHeaderFile(activeIDDeclsBuff, tupleTypeBuff, messageTypeBuff, deliverMsgBuff);

		/* Generate site specific header files */
		doGenerateIndividualHeaderFiles(configs, activeIDDeclsBuff, tupleTypeBuff, 
				messageTypeBuff, deliverMsgBuff);
    }


    /**
     * Generates the declarations for the ActiveMessageIDs used by the query plan
     * for the header file.
     * @param activeIDDeclsBuff  Buffer to store the AM ID declarations
     */
	private void addAMConstants(final StringBuffer activeIDDeclsBuff) {
		Boolean first = true;

		if (!ActiveMessageIDGenerator.isEmpty()) {
		    activeIDDeclsBuff.append("enum" + "\n");
		    activeIDDeclsBuff.append("{ " + "\n");
		    final Iterator<String> amIter = ActiveMessageIDGenerator
			    .activeMessageIDKeyIterator();

		    while (amIter.hasNext()) {
				final String id = amIter.next();
					if (first) {
					    activeIDDeclsBuff.append("\t" + id + " = "
						    + ActiveMessageIDGenerator.getactiveMessageID(id));
					    first = false;
					} else {
					    activeIDDeclsBuff.append(",\n \t" + id + " = "
						    + ActiveMessageIDGenerator.getactiveMessageID(id)
						    + "\n");
					}
			    }
		    activeIDDeclsBuff.append("\n}; " + "\n");
		}
	}


	/**
	 * Generates the operator tuple structs used by the query plan, for the header
	 * file.
	 * @param tupleTypeBuff Buffer to store all tuple structs in the DAF.
	 * @param op The operator for which to generate a struct.
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	private void addOperatorTupleStructs(final StringBuffer tupleTypeBuff, 
	final SensornetOperator op) 
	throws SchemaMetadataException, TypeMappingException {

		if (!(op instanceof SensornetExchangeOperator) && (!op.getLogicalOperator().isRecursive())) {
			tupleTypeBuff.append("// Tuple output type for operator "
				+ op.getID() + "\n");
			tupleTypeBuff.append("// " + op.toString() + "\n");
			tupleTypeBuff.append("// size = " +
					CodeGenUtils.outputTypeSize.get(
					CodeGenUtils.generateOutputTupleType(op))+ " bytes\n\n");

			
			if (tosVersion==1) {
				tupleTypeBuff.append("typedef struct ");
			} else {
				tupleTypeBuff.append("typedef nx_struct ");
			}
			tupleTypeBuff.append(CodeGenUtils.generateOutputTupleType(op) + " {\n");
		}

		final List <Attribute> attributes = op.getAttributes();
		for (int i = 0; i < attributes.size(); i++) {
		    String attrName = CodeGenUtils.getNescAttrName(attributes.get(i));

			final AttributeType attrType = attributes.get(i).getType();

			String nesCType = attrType.getNesCName();

			if (tosVersion==2) {
				nesCType = "nx_"+nesCType;
			}
			if (!(op instanceof SensornetExchangeOperator) && (!op.isRecursive())) {
				tupleTypeBuff.append("\t" + nesCType + " " + attrName + ";\n");
			}
		}
		
		if (!(op instanceof SensornetExchangeOperator) && (!op.isRecursive())) {
			tupleTypeBuff.append("} "
					+ CodeGenUtils.generateOutputTupleType(op) + ";\n\n");
			tupleTypeBuff.append("typedef "
					+ CodeGenUtils.generateOutputTupleType(op) + "* "
					+ CodeGenUtils.generateOutputTuplePtrType(op)
					+ ";\n\n\n");
		}
	}


	/**
	 * Generates the message tuple structs used by the query plan, for the
	 * header file.
	 * @param messageTypeBuff Buffer to store all message structs.
	 * @param op The operator for which to generate a message struct
	 * @throws OptimizationException 
	 */
	private void addRadioMessageStructs(final StringBuffer messageTypeBuff,
			final SensornetOperator op) throws OptimizationException {
		if ((op instanceof SensornetExchangeOperator)
		    && (!op.getLeftChild().getContainingFragment().isRecursive())) {

			final String fragID = op.getLeftChild().getFragID();

			final int tupleSize = CodeGenUtils.outputTypeSize.get(
				CodeGenUtils.generateOutputTupleType(op)).intValue();
			final int numTuplesPerMessage = ExchangePart
				.computeTuplesPerMessage(tupleSize, this.costParams);
			assert (numTuplesPerMessage > 0);

			messageTypeBuff.append("// Message output type for Fragment "
				+ fragID + " (operator " + op.getID() + ")\n");
			
			if (tosVersion==1) {
				messageTypeBuff.append("typedef struct ");
			} else {
				messageTypeBuff.append("typedef nx_struct ");
			}
				
			messageTypeBuff.append(CodeGenUtils.generateMessageType(op) + " {\n");
			messageTypeBuff.append("\tTupleFrag" + fragID + " tuples["
				+ numTuplesPerMessage + "];\n");
			messageTypeBuff.append("}"
				+ CodeGenUtils.generateMessageType(op) + ";\n\n");
			messageTypeBuff.append("typedef "
				+ CodeGenUtils.generateMessageType(op) + " *"
				+ CodeGenUtils.generateMessagePtrType(op) + ";\n\n\n");
		}
	}


	/**
	 * Generates the deliver message struct, for the header file.
	 * @return
	 */
	private StringBuffer t1AddDeliverMessageStruct() {
		StringBuffer deliverMsgBuff = new StringBuffer();
		deliverMsgBuff.append("#define DELIVER_PAYLOAD_SIZE 28\n\n");
		deliverMsgBuff.append("typedef struct DeliverMessage {\n");
		deliverMsgBuff.append("\tchar text[DELIVER_PAYLOAD_SIZE];\n");
		deliverMsgBuff.append("} DeliverMessage;\n\n");
		deliverMsgBuff.append("typedef DeliverMessage* DeliverMessagePtr;\n\n");
		return deliverMsgBuff;
	}

	/**
	 * Generates the deliver message struct, for the header file.
	 * @return
	 * @throws OptimizationException 
	 */
	private StringBuffer t2AddDeliverMessageStruct(SensornetOperator op) throws OptimizationException {
		final int tupleSize = CodeGenUtils.outputTypeSize.get(
				CodeGenUtils.generateOutputTupleType(op)).intValue();
		final int numTuplesPerMessage = ExchangePart
		.computeTuplesPerMessage(tupleSize, costParams);
		assert (numTuplesPerMessage > 0);
		
		StringBuffer deliverMsgBuff = new StringBuffer();
		deliverMsgBuff.append("typedef nx_struct DeliverMessage {\n");
		deliverMsgBuff.append("\t" + CodeGenUtils.generateOutputTupleType(op.getLeftChild()) + " tuples["
			+ numTuplesPerMessage + "];\n"); //use child type because deliver doesn't change tuple type
		deliverMsgBuff.append("} DeliverMessage;\n\n");
		
		deliverMsgBuff.append("typedef DeliverMessage* DeliverMessagePtr;\n\n");
		return deliverMsgBuff;
	}
	

	/**
	 * Generates an Avrora header file for each site.
	 * @param configs The collection of nesC configuration for each site.
	 * @param nescOutputDir The nesC output directory for generated code.
	 * @param tosVersion The TinyOS version which nesC is being generated for.
	 * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
	 * @param activeIDDeclsBuff Buffer which stores ActiveID constant decls
	 * @param tupleTypeBuff Buffer which stores all tuple structs in the DAF.
	 * @param messageTypeBuff Buffer which stores all message structs.
	 * @param deliverMsgBuff Buffer which stores the deliver message struct.
	 * @throws IOException
	 */
	private void doGenerateIndividualHeaderFiles(final HashMap<Site, NesCConfiguration> configs,
			final StringBuffer activeIDDeclsBuff, final StringBuffer tupleTypeBuff, 
			final StringBuffer messageTypeBuff, 
			StringBuffer deliverMsgBuff)
			throws IOException {
		if (!(tossimFlag || combinedImage)) {
		    final Iterator<Site> siteIter = configs.keySet().iterator();
		    while (siteIter.hasNext()) {
				final String siteID = siteIter.next().getID();
				final String fname = nescOutputDir + targetName +"/mote"
					+ siteID + "/QueryPlan.h";
				doGenerateHeaderFile(activeIDDeclsBuff, tupleTypeBuff, messageTypeBuff,
						deliverMsgBuff, fname);
		    }
		}
	}


	/**
	 * Generates a single Tossim header file for all sites.
	 * @param nescOutputDir The nesC output directory for generated code.
	 * @param tosVersion The TinyOS version which nesC is being generated for.
	 * @param combinedImage Indicates whether code for Tossim simulator needs to be generated.
	 * @param activeIDDeclsBuff Buffer which stores ActiveID constant decls 
	 * @param tupleTypeBuff Buffer which stores all tuple structs in the DAF.
	 * @param messageTypeBuff Buffer which stores all message structs.
	 * @param deliverMsgBuff Buffer which stores the deliver message struct.
	 * @throws IOException
	 */
	private void doGenerateCombinedHeaderFile(final StringBuffer activeIDDeclsBuff, 
	final StringBuffer tupleTypeBuff,
	final StringBuffer messageTypeBuff, StringBuffer deliverMsgBuff) throws IOException {

		if (this.tossimFlag || this.combinedImage) {
		    final String fname = nescOutputDir
			    + targetName +"/QueryPlan.h";
			doGenerateHeaderFile(activeIDDeclsBuff, tupleTypeBuff, messageTypeBuff,
					deliverMsgBuff, fname);
		}
	}

	/**
	 * Generates header file at specific location.
	 * @param tupleTypeBuff Buffer which stores all tuple structs in the DAF.
	 * @param messageTypeBuff Buffer which stores all message structs.
	 * @param deliverMsgBuff Buffer which stores the deliver message struct.
	 * @param fname The path of the file to be written.
	 * @throws IOException
	 */
	private void doGenerateHeaderFile(
			final StringBuffer activeIDDeclsBuff, final StringBuffer tupleTypeBuff,
			final StringBuffer messageTypeBuff, StringBuffer deliverMsgBuff,
			final String fname) throws IOException {
		final PrintWriter out = new PrintWriter(new BufferedWriter(
			new FileWriter(fname)));

		out.println("#ifndef __QUERY_PLAN_H__\n");
		out.println("#define __QUERY_PLAN_H__\n");
		
		if (this.enablePrintf) {
			if (targetName.equals("tmotesky_t2")) {
				out.println("#include \"printf.h\"\n");
			} else if (targetName.equals("z1")) {
				out.println("#include \"printfZ1.h\"\n");
			}
		}
		
		out.println("enum {NULL_EVAL_EPOCH = -1};\n");

		out.println(activeIDDeclsBuff.toString()+"\n");
		
		if (tossimFlag) {
		    out.println("uint32_t sysTime = 0;\n");
		    out.println("uint8_t synchronizing = TRUE;\n");
		}

		out.println(tupleTypeBuff);
		out.println(messageTypeBuff);
		out.println(deliverMsgBuff);

		out.println("\n#endif\n\n");
		out.close();
	}


	/**
	 * Generates the typed interface files for the query plan.
	 * @param plan
	 * @param configs
	 * @param nescOutputDir
	 * @param tosVersion
	 * @param combinedImage
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
    private void generateTypedInterfaces(final HashMap<Site, NesCConfiguration> configs) throws IOException, URISyntaxException {

    	final Iterator<SensornetOperator> opIter = plan.getDAF()
		.operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = (SensornetOperator) opIter.next();

		    if (op instanceof SensornetExchangeOperator) {
		    	continue;
		    } else if (op.isFragmentRoot()) {

		    	/* Between fragments a GetTuples and PutTuples interface is needed */
		    	generateInterFragmentInterfaces(configs, op);
		    } else {

		    	/* Within a fragment only a GetTuples interface is needed */
				generateIntraFragmentInterfaces(configs, op);
		    }
		}
    }


    /**
	 * Generates typed interfaces within fragments, i.e., just a GetTuples
     * interface.
     * @param configs
     * @param nescOutputDir
     * @param tosVersion
     * @param tossimFlag
     * @param op
     * @throws IOException
     * @throws URISyntaxException 
     */
	private void generateIntraFragmentInterfaces(final HashMap<Site, NesCConfiguration> configs,
	final SensornetOperator op)
	throws IOException, URISyntaxException {

	    final HashMap<String, String> replacements = new HashMap<String, String>();

		replacements.put("__INTERFACE_NAME__", CodeGenUtils
			.generateGetTuplesInterfaceInstanceName(op));
		replacements.put("__TUPLE_TYPE_PTR__", "TupleOp" + op.getID()
			+ "Ptr");

		generateTypedInterface(INTERFACE_GET_TUPLES, configs, op, replacements);
	}


	/**
     * Generates typed interfaces between fragments, i.e., a GetTuples and PutTuples
     * interface.
	 * @param configs
	 * @param nescOutputDir
	 * @param tosVersion
	 * @param tossimFlag
	 * @param op
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private void generateInterFragmentInterfaces(final HashMap<Site, 
	NesCConfiguration> configs, final SensornetOperator op)
	throws IOException, URISyntaxException {

	    final HashMap<String, String> replacements = new HashMap<String, String>();

		// For roots of fragments, the tuple name is TupleFrag{fragId}
		// Need both requestData and TrayPut interfaces

		replacements.put("__PUT_INTERFACE_NAME__", CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(op
						.getContainingFragment()));
		replacements.put("__INTERFACE_NAME__", CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(op
						.getContainingFragment()));
		replacements.put("__TUPLE_TYPE_PTR__", "TupleFrag"
				+ op.getFragID() + "Ptr");
		replacements
			.put("__TUPLE_TYPE__", "TupleFrag" + op.getFragID());

		generateTypedInterface(INTERFACE_GET_TUPLES, configs, op, replacements);
		generateTypedInterface(INTERFACE_PUT_TUPLES, configs, op, replacements);
	}


	/**
	 * Generates a typed interface according to a given set of replacements.
	 * @param interfaceName
	 * @param configs
	 * @param nescOutputDir
	 * @param tosVersion
	 * @param tossimFlag
	 * @param op
	 * @param replacements
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private void generateTypedInterface(String interfaceName,
	final HashMap<Site, NesCConfiguration> configs,
	final SensornetOperator op, final HashMap<String, String> replacements)
	throws IOException, URISyntaxException {

		//INTERFACE_GET_TUPLES
		String interfaceInstanceName = CodeGenUtils.
			generateGetTuplesInterfaceInstanceName(op)+ ".nc";
		if (interfaceName==INTERFACE_PUT_TUPLES) {
			interfaceInstanceName = CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(
				op.getContainingFragment())+".nc";
		}


		if (!(tossimFlag || combinedImage)) {
		    final Iterator<Site> siteIter = configs.keySet().iterator();
		    while (siteIter.hasNext()) {
				final Site site = siteIter.next();

				Template.instantiate(NESC_INTERFACES_DIR +
						"/" + interfaceName + ".nc",
						nescOutputDir + targetName +"/mote" +
						site.getID() + "/" +
						interfaceInstanceName, replacements);
		    }
		}
		if (tossimFlag || combinedImage) {
		    Template.instantiate(NESC_INTERFACES_DIR +
		    		"/" + interfaceName + ".nc",
				    nescOutputDir + targetName +"/" +
				    interfaceInstanceName, replacements);
		}
	}


    /**
     * Helper function for methods resonsible for generating miscellaneous files.
     * @param interfaceName
     * @param plan
     * @param nescOutputDir
     * @param nodeDir
     * @throws IOException
     * @throws URISyntaxException 
     */
    private void copyInterfaceFile(final String interfaceName, 
    final String nodeDir) throws IOException, URISyntaxException {
    	Template.instantiate(NESC_INTERFACES_DIR + "/"
    		+ interfaceName + ".nc", nescOutputDir + nodeDir
    		+ "/" + interfaceName + ".nc");
        }

    /**
     * Generates Miscellaneous files (make files, and other supporting files).
     * @param numNodes
     * @throws IOException
     * @throws URISyntaxException 
     */
    private void copyMiscFiles(int numNodes) throws IOException, URISyntaxException {

		if (!tossimFlag && !combinedImage) {
				generateIndividualMiscFiles();
		} else if (combinedImage && !tossimFlag) {
			generateCombinedMiscFiles();
		} else if (tossimFlag) {
			generateTossimMiscFiles(numNodes);
		}
    }


    /**
     * Generates Miscellaneous files for Avrora.
     * @throws IOException
     * @throws URISyntaxException 
     */
	private void generateIndividualMiscFiles() throws IOException, URISyntaxException {
		final Iterator<Site> siteIter = plan
		    .siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
		    final String siteID = siteIter.next().getID();
			final String nodeDir = targetName+"/mote" + siteID;
			copyInterfaceFile(INTERFACE_DO_TASK, nodeDir);

			generateMakefiles(nescOutputDir + nodeDir,
				"QueryPlan" + siteID);

		    if (tosVersion==2) {
		    	Template.instantiate(
					    NESC_MISC_FILES_DIR + "/itoa.h",
					    nescOutputDir + nodeDir
						    + "/itoa.h");
		    }
		    
		    if (this.includeDeluge) {
		    	Template.instantiate(
					    NESC_MISC_FILES_DIR + "/volumes-stm25p.xml",
					    nescOutputDir + nodeDir +"/volumes-stm25p.xml");		    	
		    }
		}
	}

	private void generateCombinedMiscFiles() throws IOException, URISyntaxException {

			copyInterfaceFile(INTERFACE_DO_TASK, targetName +"/");

		    generateMakefiles(nescOutputDir+ targetName, "QueryPlan");

		    if (tosVersion==2) {
		    	Template.instantiate(
					    NESC_MISC_FILES_DIR + "/itoa.h",
					    nescOutputDir + targetName +"/itoa.h");
		    }
		    
		    if (this.includeDeluge) {
		    	Template.instantiate(
					    NESC_MISC_FILES_DIR + "/volumes-stm25p.xml",
					    nescOutputDir + targetName +"/volumes-stm25p.xml");		    	
		    }
		}
	

	/**
	 * Generates Miscellaneous files for Tossim.
	 * @param plan The query plan which code is being generated for.
	 * @param nescOutputDir
	 * @param qos
	 * @param numNodes
	 * @param tosVersion
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private void generateTossimMiscFiles(int numNodes) throws IOException, URISyntaxException {

		copyInterfaceFile(INTERFACE_DO_TASK, targetName +"/");

	    generateMakefiles(nescOutputDir+ targetName, "QueryPlan");

	    if (tosVersion==2) {
	    	Template.instantiate(
				    NESC_MISC_FILES_DIR + "/itoa.h",
				    nescOutputDir + targetName +"/itoa.h");

		    HashMap<String, String> replacements = new HashMap<String, String>();
		    //TODO: Get Query Duration from QoS?
		    //			long duration = qos.getQueryDuration();
		    long duration = 100000; //hard-coded value for now...
			replacements.put("__SIMULATION_DURATION__", new Long(duration).toString());
			replacements.put("__NUM_NODES__", new Integer(numNodes).toString());
			Template.instantiate(
		    		NESC_MISC_FILES_DIR + "/runTossim.py",
		    		nescOutputDir + targetName +"/runTossim.py", replacements);

			Template.instantiate(
				    NESC_MISC_FILES_DIR + "/meyer-light.txt",
				    nescOutputDir + targetName +"/meyer-light.txt");
			
			Template.instantiate(
				    NESC_MISC_FILES_DIR + "/RandomSensorC.nc",
				    nescOutputDir + targetName +"/RandomSensorC.nc");
		}
	}


	/**
	 * Generates Makefile and MakeRules files in the given directory.
	 * @param dir The directory where the files are to be created.
	 * @param mainConfigName The name of main nesC configuration.
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private void generateMakefiles(final String dir, String mainConfigName) throws IOException, URISyntaxException {
		// Makefile
		HashMap<String, String> replacements = new HashMap<String, String>();
		replacements.put("__MAIN_CONFIG_NAME__", mainConfigName);
		if (this.includeDeluge && tossimFlag == false) {
			replacements.put("__DELUGE__","BOOTLOADER=tosboot");			
		} else {
			replacements.put("__DELUGE__","");
		}
		if (this.enablePrintf && targetName.equals("tmotesky_t2")) {
			replacements.put("__PRINTF__", "CFLAGS += -I$(TOSDIR)/lib/printf");
		} else if (this.enablePrintf && targetName.equals("z1")) {
			replacements.put("__PRINTF__", "CFLAGS += -DPRINTFUART_ENABLED");
		} else {
			replacements.put("__PRINTF__","");			
		}
		
		
		Template.instantiate(NESC_MISC_FILES_DIR
			+ "/Makefile", dir
			+ "/Makefile", replacements);

		Template.instantiate(
		    NESC_MISC_FILES_DIR + "/Makerules",
		    dir + "/Makerules");
	}




    /**
     * Main method for NesCGeneration class.
     * @param plan The query plan which code is being generated for.
     * @param qos The user-specified quality-of-service requirements.
     * @param sink The sink node of the network.
     * @param nescOutputDir The nesC output directory for generated code.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @throws OptimizationException
     * @throws CodeGenerationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    public void doNesCGeneration(SensorNetworkQueryPlan qep)
	    throws OptimizationException, CodeGenerationException, SchemaMetadataException, TypeMappingException {

    	this.plan = qep;
		computeOperatorTupleSizes();
    	
    	try {
			/* Generate inter-fragment configurations */
			HashMap<Site, NesCConfiguration> siteConfigs = null;
			siteConfigs = generateSiteConfigurations();
			final NesCConfiguration combinedConfig =
				generateCombinedConfiguration(siteConfigs);

		    /* Generate an intra-fragment configuration (i.e., operator
		     * granularity) for each fragment instance */
		    generateFragmentConfigurations(siteConfigs);

		    /* Add LED support for debugging */
			if (this.enableLeds) {
				addLEDs(siteConfigs, combinedConfig);
			}		    
		    
		    /* Instantiate code for the top-level configuration(s); the nested
		     * components and configurations are instantiated recursively */
		    if (tossimFlag || combinedImage) {
			    instantiateCombinedConfiguration(combinedConfig);
		    } else {
			    instantiateSiteConfigurations(siteConfigs);
		    }

		    /* Generate QueryPlan.h file with constants and data
		     * structures etc.*/
		    generateHeaderFiles(siteConfigs);

		    /* Generate the interface files */
		    generateTypedInterfaces(siteConfigs);

		    /* Copy interface files over from templates dir to nesC output
		     * dirs */
		    copyMiscFiles(siteConfigs.size());

		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage(), e);
		    throw new CodeGenerationException(e);
		} 
    }
}
