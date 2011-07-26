/****************************************************************************\ 
*                                                                            *
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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IDAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSingleStepAggregationOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;
import uk.ac.manchester.cs.snee.sncb.tos.AMRecieveComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AMSendComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AcquireComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ActiveMessageIDGenerator;
import uk.ac.manchester.cs.snee.sncb.tos.AggrEvalComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AggrInitComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AggrMergeComponent;
import uk.ac.manchester.cs.snee.sncb.tos.AggrSingleStepComponent;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenUtils;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.tos.DeliverComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ExchangeProducerComponent;
import uk.ac.manchester.cs.snee.sncb.tos.FragmentComponent;
import uk.ac.manchester.cs.snee.sncb.tos.JoinComponent;
import uk.ac.manchester.cs.snee.sncb.tos.LedComponent;
import uk.ac.manchester.cs.snee.sncb.tos.LocalTimeComponent;
import uk.ac.manchester.cs.snee.sncb.tos.MainComponent;
import uk.ac.manchester.cs.snee.sncb.tos.NesCComponent;
import uk.ac.manchester.cs.snee.sncb.tos.NesCConfiguration;
import uk.ac.manchester.cs.snee.sncb.tos.NodeControllerComponent;
import uk.ac.manchester.cs.snee.sncb.tos.ProjectComponent;
import uk.ac.manchester.cs.snee.sncb.tos.QueryPlanModuleComponent;
import uk.ac.manchester.cs.snee.sncb.tos.RXComponent;
import uk.ac.manchester.cs.snee.sncb.tos.RadioComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SelectComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SensorComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SensorComponentUtils;
import uk.ac.manchester.cs.snee.sncb.tos.SerialAMSendComponent;
import uk.ac.manchester.cs.snee.sncb.tos.SerialStarterComponent;
import uk.ac.manchester.cs.snee.sncb.tos.TXComponent;
import uk.ac.manchester.cs.snee.sncb.tos.Template;
import uk.ac.manchester.cs.snee.sncb.tos.TimerComponent;
import uk.ac.manchester.cs.snee.sncb.tos.TrayComponent;
import uk.ac.manchester.cs.snee.sncb.tos.WindowComponent;

/**
 *
 * Code Generation
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

    private static String COMPONENT_LEDS;
    
    public static String INTERFACE_DO_TASK;

    public static String INTERFACE_GET_TUPLES;

    public static String INTERFACE_PUT_TUPLES;

    public static String INTERFACE_RECEIVE;

    public static String INTERFACE_SEND;

    public static String INTERFACE_TIMER;

    private static String INTERFACE_READ;

    private static String INTERFACE_LEDS;
    
    // TinyOS2 Only
    private static String COMPONENT_AGENDA_TIMER;

    private static String COMPONENT_LOCAL_TIME;
    
    private static String COMPONENT_RADIOTX;

    private static String COMPONENT_RADIORX;
    
    public static String COMPONENT_SERIALTX;
    
    public static String COMPONENT_SERIALRX;
    
    public static String COMPONENT_SERIAL_STARTER;

    public static String COMPONENT_NODE_CONTROLLER;
    
    private static String INTERFACE_AMPACKET;
    
    private static String INTERFACE_BOOT;

    public static String INTERFACE_LOCAL_TIME;

    private static String INTERFACE_PACKET;

    public static String INTERFACE_SNOOZE;

    public static String INTERFACE_SPLITCONTROL;
    
    public static String INTERFACE_NETWORKSTATE;
    
    public static String TYPE_TMILLI;

    public static String TYPE_READ;


    private CodeGenTarget target;
    
    private SensorNetworkQueryPlan plan;
    
    private boolean tossimFlag = true;
    
    private String targetDirName;
    
    private boolean combinedImage = false;
    
    private String nescOutputDir;
    
    private CostParameters costParams;
    
    private MetadataManager metadata;
    
	private boolean controlRadio;

	private boolean enablePrintf; //not tested

	private boolean enableLeds;

	private boolean debugLeds;
	
	private boolean useNodeController;
	    
	private boolean usesCustomSeaLevelSensor = false;
	
    public TinyOSGenerator(CodeGenTarget codeGenTarget,
    boolean combinedImage, String nescOutputDir, MetadataManager metadata, boolean controlRadio,
    boolean enablePrintf, boolean enableLeds, boolean debugLeds, 
    boolean useNodeController)
    throws IOException, SchemaMetadataException, TypeMappingException {
		this.target = codeGenTarget;
		this.targetDirName = codeGenTarget.toString().toLowerCase();
		this.controlRadio =controlRadio;
		
    	if (target==CodeGenTarget.TELOSB_T2) {
        	this.tossimFlag = false;
    		this.useNodeController = useNodeController;
        	this.combinedImage = combinedImage;
    		this.controlRadio = false; // leads to too much packet loss, currently
    	}
    	if (target==CodeGenTarget.AVRORA_MICA2_T2) {
        	this.tossimFlag = false;
    		this.useNodeController = false; // incompatible
        	this.combinedImage = combinedImage;
    	}
    	if (target==CodeGenTarget.AVRORA_MICAZ_T2) {
        	this.tossimFlag = false;
    		this.useNodeController = false; // incompatible
        	this.combinedImage = combinedImage;
    	}    	
    	if (target==CodeGenTarget.TOSSIM_T2) {
        	this.tossimFlag = true;
    		this.useNodeController = false; // incompatible
        	this.combinedImage = true; // doesn't work otherwise
    		this.controlRadio = false; // incompatible
    	}
    	
    	this.nescOutputDir = nescOutputDir;
		this.costParams = metadata.getCostParameters();
		this.metadata = metadata;


		this.enablePrintf = enablePrintf;
		this.enableLeds = enableLeds;
		this.debugLeds = debugLeds;
		
    	initConstants();

    	NESC_TEMPLATES_DIR = "etc/sncb/templates/tos2/";
    	NESC_COMPONENTS_DIR = NESC_TEMPLATES_DIR + "components";
    	NESC_INTERFACES_DIR = NESC_TEMPLATES_DIR + "interfaces";
    	NESC_MISC_FILES_DIR = NESC_TEMPLATES_DIR + "misc";
    }

    /**
     * Initialises the constants which specify the component names. These vary
     * depending on whether we are generating TinyOS1/TinyOS2 nesC code, and
     * whether it is for the Tossim simulator or not.
     *
     */
    private void initConstants() {
	    COMPONENT_AGENDA_TIMER = "AgendaTimer";
	    COMPONENT_LOCAL_TIME = "LocalTimeMilliC";
	    COMPONENT_MAIN = "MainC";
	    COMPONENT_NODE_CONTROLLER = "CommandServerAppC";
	    COMPONENT_QUERY_PLAN = "QueryPlanC";
	    COMPONENT_QUERY_PLANC = "QueryPlan";  //AppC
	    COMPONENT_RADIO = "ActiveMessageC";
	    COMPONENT_RADIOTX = "AMSenderC";
	    COMPONENT_RADIORX = "AMRecieverC";	    	
	    COMPONENT_LEDS = "LedsC";
	    COMPONENT_SERIALTX = "SerialAMSenderC";
	    COMPONENT_SERIALRX = "SerialAMReceiverC";
	    COMPONENT_SERIAL_STARTER = "SerialStarterC";
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
	    INTERFACE_NETWORKSTATE = "NetworkState";
	    TYPE_TMILLI = "TMilli";
	    TYPE_READ = "uint16_t";
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
		LedComponent ledComp = new LedComponent(COMPONENT_LEDS, config, tossimFlag);
		config.addComponent(ledComp);
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_LEDS, INTERFACE_LEDS);
				
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
	 * Wire the fragments with trays, radio receive and transmit.
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
			.generateName(destFrag, currentSite);
		config.addWiring(destFragCompName,
			trayName,
			CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(sourceFrag),
			CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(sourceFrag),
			INTERFACE_GET_TUPLES);


		addRXComponent(config, exchPart,
					sourceFrag, destFrag, destSiteID, trayName);
	}


	/**
	 * Add a remote producer.
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
			.generateName(sourceFrag, currentSite);
		config.addWiring(sourceFragCompName,
			trayName,
			CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(sourceFrag),
			CodeGenUtils
				.generatePutTuplesInterfaceInstanceName(sourceFrag),
			INTERFACE_PUT_TUPLES);

			addTXComponent(config, exchPart, sourceFrag,
					destFrag, destSiteID, trayName);
	}


	/**
	 * Add relay.
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
		
		addRXComponent(config, exchPart, sourceFrag, destFrag, 
				destSiteID, trayName);
		addTXComponent(config, exchPart, sourceFrag, destFrag, 
				destSiteID, trayName);
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
			.generateName(destFrag, currentSite);
		config.addWiring(
				destFragCompName,
				trayName,
				CodeGenUtils
					.generateGetTuplesInterfaceInstanceName(sourceFrag),
				CodeGenUtils
					.generateGetTuplesInterfaceInstanceName(sourceFrag),
				INTERFACE_GET_TUPLES);

		final String sourceFragCompName = FragmentComponent
			.generateName(sourceFrag, currentSite);
		config.addWiring(sourceFragCompName, trayName,
				CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
				CodeGenUtils.generatePutTuplesInterfaceInstanceName(sourceFrag),
				INTERFACE_PUT_TUPLES);
	}


	/**
	 * Creates a tray to buffer tuples for an exachange part
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
				destSiteID, currentSite, config, plan, 
				tossimFlag, costParams, debugLeds, target);
		
		trayComp = (TrayComponent) config.addComponent(trayComp);
		// tray may already exist
		final String trayName = trayComp.getID();
		return trayName;
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
    private HashMap<Site, NesCConfiguration> generateSiteConfigurations()
	    throws IOException, CodeGenerationException {

		final HashMap<Site, NesCConfiguration> nodeConfigs = new HashMap<Site, NesCConfiguration>();

		final Iterator<Site> siteIter = plan.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			final Site currentSite = siteIter.next();
			final String currentSiteID = currentSite.getID();

			/* Instantiate the top-level site configuration */
			final NesCConfiguration config = new NesCConfiguration(
				COMPONENT_QUERY_PLANC + currentSiteID, //$
				plan, currentSite, tossimFlag);

			/* Add the components which are always needed */
			addMainSiteComponents(config);

			/* Optional components for Led debugging not implemented yet for TOS2 */

			/* Power Management components not implemented yet for TOS2*/

			/* Functionality for changing radio transmit power not implemented yet for TOS2*/

			/* Add the fragments which have been placed onto this site */
			addSiteFragments(currentSite, config);

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
	private void addMainSiteComponents(final NesCConfiguration config) throws CodeGenerationException {
		MainComponent mainComp = new MainComponent(COMPONENT_MAIN, config,
			tossimFlag);
		config.addComponent(mainComp);

		int sink = plan.getGateway();
		QueryPlanModuleComponent queryPlanModuleComp =
			new QueryPlanModuleComponent(COMPONENT_QUERY_PLAN, config,
			plan, sink, (tossimFlag || combinedImage), targetDirName,
			costParams, controlRadio, enablePrintf, enableLeds,
			debugLeds, useNodeController, target);
		config.addComponent(queryPlanModuleComp);

		TimerComponent timerComp = new TimerComponent(		//$
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
		
		if (this.useNodeController) {
			addNodeControllerComponents(config);
		}
	}


	private void addNodeControllerComponents(NesCConfiguration config)
	throws CodeGenerationException {
		NodeControllerComponent nodeControllerComp = 
			new NodeControllerComponent(COMPONENT_NODE_CONTROLLER, config, 
					this.tossimFlag);
		config.addComponent(nodeControllerComp);
		
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_NODE_CONTROLLER, 
				INTERFACE_SPLITCONTROL, "CommandServerControl", 
				INTERFACE_SPLITCONTROL);
		config.addWiring(COMPONENT_QUERY_PLAN, COMPONENT_NODE_CONTROLLER, 
		
				INTERFACE_NETWORKSTATE, "CommandServerUpdate", 
				INTERFACE_NETWORKSTATE);
	}

    /**
     * TOS2: Add the fragments which have been placed onto this site.
     * @param tosVersion The TinyOS version which nesC is being generated for.
     * @param tossimFlag Indicates whether code for Tossim simulator needs to be generated.
     * @param currentSite The site for which code is being generated.
     * @param config The nesC configuration which components/wirings are being added to.
     * @throws CodeGenerationException
     */
	private void addSiteFragments(final Site currentSite, final NesCConfiguration config)
			throws CodeGenerationException {
		final Iterator<Fragment> fragIter = currentSite.getFragments()
			.iterator();
		while (fragIter.hasNext()) {
			final Fragment frag = fragIter.next();

			/* Add component for current fragment */
			final FragmentComponent fragComp = new FragmentComponent(
				frag, config, tossimFlag);
			config.addComponent(fragComp);

			/* Wire fragment to main query plan component */
			config.addWiring(COMPONENT_QUERY_PLAN, fragComp.getID(),
				INTERFACE_DO_TASK, CodeGenUtils
					.generateUserAsDoTaskName(frag, currentSite),
				INTERFACE_DO_TASK);

			/* Wire fragment to hardware devices as required */
			wireFragToDevices(currentSite, config, frag, fragComp);
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
	private void wireFragToDevices(final Site currentSite, 
	final NesCConfiguration config, final Fragment frag, final FragmentComponent fragComp)
	throws CodeGenerationException {
		final Iterator<SensornetOperator> opIter = frag
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
			final SensornetOperator op = opIter.next();
			if (op instanceof SensornetAcquireOperator) {

				/* Wire fragment component to the sensor component */
				wireFragToSensors(currentSite, config, fragComp, op);
				
//TODO: Should be viewed as another type of sensor in the physical schema				
//				if (this.showLocalTime) {
//					wireFragToLocalTime(currentSite, config, fragComp, op);
//				}
				
			} else if (op instanceof SensornetDeliverOperator) {
				
				SerialStarterComponent serialStartComp = new SerialStarterComponent (
							COMPONENT_SERIAL_STARTER, config, tossimFlag);
				config.addComponent(serialStartComp);					

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
	private void wireFragToSensors(final Site currentSite, 
	final NesCConfiguration config, final FragmentComponent fragComp, final SensornetOperator op)
	throws CodeGenerationException {
		AcquireOperator acqOp = (AcquireOperator) op.getLogicalOperator();
		List<Attribute> attributes = acqOp.getInputAttributes();
		
		int sensorID = 0;
		for (Attribute attr : attributes) {
			if ((attr instanceof EvalTimeAttribute) ||
					(attr instanceof IDAttribute)) {
				continue;
			}

			SensorType sensorType = metadata.getAttributeSensorType(attr);
			String nesCComponentName = SensorComponentUtils.
				getNesCComponentName(sensorType, this.target);
			String nesCInterfaceName = SensorComponentUtils.
				getNesCInterfaceName(sensorType, this.target);
			
			if (sensorType == SensorType.SEA_LEVEL) {
				usesCustomSeaLevelSensor = true;
			}
			
			final SensorComponent sensorComp = new SensorComponent(
					currentSite, ""+sensorID, nesCComponentName, config, "",
					tossimFlag);
			config.addComponent(sensorComp);
			final String sensorName = sensorComp.getID();

			config.addWiring(fragComp.getID(), sensorName,
					INTERFACE_READ, TYPE_READ,
					"Op" + op.getID() + INTERFACE_READ + sensorID, nesCInterfaceName);
			
			sensorID++;
		}
	}

	private void wireFragToLocalTime(final Site currentSite, 
	final NesCConfiguration config, final FragmentComponent fragComp, final SensornetOperator op)
	throws CodeGenerationException {

		final LocalTimeComponent localTimeComp = new LocalTimeComponent(
					COMPONENT_LOCAL_TIME, config, tossimFlag);
			config.addComponent(localTimeComp);

			config.addWiring(fragComp.getID(), COMPONENT_LOCAL_TIME,
					INTERFACE_LOCAL_TIME, TYPE_TMILLI, INTERFACE_LOCAL_TIME, INTERFACE_LOCAL_TIME);
	}

	private void addTXComponent(final NesCConfiguration config, final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {
		
		final String txActiveMessageIDKey =
			ActiveMessageIDGenerator.getActiveMessageIDKey(
					sourceFrag.getID(),
					destFrag.getID(),
					destSiteID,
					exchPart.getCurrentSite().getID());

		final TXComponent txComp = new TXComponent(
				sourceFrag, destFrag,
				exchPart.getDestSite(),
				exchPart.getNext().getCurrentSite(),
				config, plan, tossimFlag, costParams, debugLeds, target);

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

	
	private void addRXComponent(final NesCConfiguration config, 
			final ExchangePart exchPart,
			final Fragment sourceFrag, final Fragment destFrag,
			final String destSiteID, final String trayName)
			throws CodeGenerationException {

		final RXComponent rxComp = new RXComponent(
			sourceFrag, destFrag,
			exchPart.getDestSite(),
			exchPart.getPrevious().getCurrentSite(),
			config, plan, tossimFlag, debugLeds, target);
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
			COMPONENT_QUERY_PLANC, plan, tossimFlag);

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
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetSingleStepAggregationOperator) {    
		    return new AggrSingleStepComponent((SensornetSingleStepAggregationOperator) op, plan,
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetAggrEvalOperator) {
		    return new AggrEvalComponent((SensornetAggrEvalOperator) op, plan,
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetAggrInitOperator) {
		    return new AggrInitComponent((SensornetAggrInitOperator) op, plan,
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetAggrMergeOperator) {
		    return new AggrMergeComponent((SensornetAggrMergeOperator) op, plan,
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetDeliverOperator) {
		    return new DeliverComponent((SensornetDeliverOperator) op, plan,
		    		config, tossimFlag, debugLeds, costParams, target);
		} else if (op instanceof SensornetExchangeOperator) {
		    return new ExchangeProducerComponent((SensornetExchangeOperator) op, plan,
			    config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetNestedLoopJoinOperator) {
		    return new JoinComponent((SensornetNestedLoopJoinOperator) op, plan, config,
		    		tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetProjectOperator) {
		    return new ProjectComponent((SensornetProjectOperator) op, plan,
		    		config, tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetSelectOperator) {
		    return new SelectComponent((SensornetSelectOperator) op, plan, config,
		    		tossimFlag, debugLeds, target);
		} else if (op instanceof SensornetWindowOperator) {
		    return new WindowComponent((SensornetWindowOperator) op, plan, config,
		    		tossimFlag, debugLeds, target);
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
					site);
				final FragmentComponent fragComp = (FragmentComponent)
					nodeConfigs.get(site).getComponent(fragName);
				final NesCConfiguration fragConfig = new NesCConfiguration(
					fragName, plan, site, tossimFlag);
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
			.generateOperatorInstanceName(rootOp, site);
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
		
		//TODO: Should be viewed as another type of sensor in the physical schema		
//		if (this.showLocalTime) {
//			addExternalLocalTimeWiring(site, frag, fragConfig);
//		}
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
					.generateOperatorInstanceName(childOp, site);
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
		    producerOp, plan, fragConfig, tossimFlag, debugLeds, target);
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
		
		fragConfig.linkToExternalProvider(rootOpName, INTERFACE_PACKET,
					"Packet", "Packet");
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
			    .generateOperatorInstanceName(op, site);
		    if (op instanceof SensornetAcquireOperator) {
			    final int numSensedAttr
			    	= ((AcquireOperator) op.getLogicalOperator()).getNumberInputAttributes();
			    for (int i = 0; i < numSensedAttr; i++) {
					    fragConfig.linkToExternalProvider(opName,
						    INTERFACE_READ, TYPE_READ, INTERFACE_READ + i,
						    "Op" + op.getID() + INTERFACE_READ + i);
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
			    .generateOperatorInstanceName(op, site);
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

		String outputDir = nescOutputDir + targetDirName+"/";
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
			String outputDir = nescOutputDir + targetDirName+"/mote" + currentSite.getID() + "/";
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
		deliverMsgBuff = addDeliverMessageStruct(plan.getDAF().getRootOperator());

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

		if (!(op instanceof SensornetExchangeOperator) && (!op.isRecursive())) {
			tupleTypeBuff.append("// Tuple output type for operator "
				+ op.getID() + "\n");
			tupleTypeBuff.append("// " + op.toString() + "\n");
			tupleTypeBuff.append("// size = " +
					CodeGenUtils.outputTypeSize.get(
					CodeGenUtils.generateOutputTupleType(op))+ " bytes\n\n");

			tupleTypeBuff.append("typedef struct ");				



			tupleTypeBuff.append(CodeGenUtils.generateOutputTupleType(op) + " {\n");
		}
		
		boolean evalTimeDone = false;
		final List <Attribute> attributes = op.getAttributes();
		for (int i = 0; i < attributes.size(); i++) {
		    String attrName = CodeGenUtils.getNescAttrName(attributes.get(i));

			final AttributeType attrType = attributes.get(i).getType();

			//avoid duplicate evalTime attributes
			if (attributes.get(i) instanceof EvalTimeAttribute) {
				if (evalTimeDone==true)
					continue;
				else
					evalTimeDone = true;
			}
			
			String nesCType;
			if (attributes.get(i) instanceof EvalTimeAttribute ||
					attributes.get(i) instanceof TimeAttribute ||
					attributes.get(i) instanceof IDAttribute ) {
				nesCType = "uint16_t";
			} else {
				nesCType = "float";
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
			
			messageTypeBuff.append("typedef struct ");
				
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
	 * @throws OptimizationException 
	 */
	private StringBuffer addDeliverMessageStruct(SensornetOperator op) throws OptimizationException {
		final int tupleSize = CodeGenUtils.outputTypeSize.get(
				CodeGenUtils.generateOutputTupleType(op)).intValue();
		final int numTuplesPerMessage = ExchangePart
		.computeTuplesPerMessage(tupleSize, costParams);
		assert (numTuplesPerMessage > 0);
		
		StringBuffer deliverMsgBuff = new StringBuffer();
		deliverMsgBuff.append("typedef struct DeliverMessage {\n");

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
				final String fname = nescOutputDir + targetDirName +"/mote"
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
			    + targetDirName +"/QueryPlan.h";
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
			if (target == CodeGenTarget.TELOSB_T2) {
				out.println("#include \"printf.h\"\n");
			}
		}
		
		out.println("enum {NULL_EVAL_EPOCH = 65535};\n");

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
						nescOutputDir + targetDirName +"/mote" +
						site.getID() + "/" +
						interfaceInstanceName, replacements);
		    }
		}
		if (tossimFlag || combinedImage) {
		    Template.instantiate(NESC_INTERFACES_DIR +
		    		"/" + interfaceName + ".nc",
				    nescOutputDir + targetDirName +"/" +
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
     * @throws UtilsException 
     */
    private void copyMiscFiles() throws IOException, URISyntaxException, UtilsException {

		if (!tossimFlag && !combinedImage) {
				generateIndividualMiscFiles();
		} else if (combinedImage && !tossimFlag) {
			generateCombinedMiscFiles();
		} else if (tossimFlag) {
			generateTossimMiscFiles();
		}
		if (this.target == CodeGenTarget.AVRORA_MICA2_T2 || 
				this.target == CodeGenTarget.AVRORA_MICAZ_T2){
			copyBlinkFiles();
		}
    }

	private void copyBlinkFiles() throws IOException, URISyntaxException {
		File dir = new File(nescOutputDir+ targetDirName + "/Blink");
		dir.mkdir();
		
		Template.instantiate(NESC_MISC_FILES_DIR + "/Blink/BlinkAppC.nc", 
				nescOutputDir+ targetDirName + "/Blink/BlinkAppC.nc");
		Template.instantiate(NESC_MISC_FILES_DIR + "/Blink/BlinkC.nc", 
				nescOutputDir+ targetDirName + "/Blink/BlinkC.nc");
		Template.instantiate(NESC_MISC_FILES_DIR + "/Blink/Makefile", 
				nescOutputDir+ targetDirName + "/Blink/Makefile");
	}

    public void copySerialStarterFiles(String dir) throws IOException, URISyntaxException {
    	Template.instantiate(NESC_MISC_FILES_DIR + "/SerialStarterC.nc",
    			dir +"/SerialStarterC.nc");
    	Template.instantiate(NESC_MISC_FILES_DIR + "/AutoStarterC.nc",
    			dir +"/AutoStarterC.nc");
    	Template.instantiate(NESC_MISC_FILES_DIR + "/AutoStarterP.nc",
    			dir +"/AutoStarterP.nc"); 	
    }
    

    /**
     * Generates Miscellaneous files for individual image QEPs.
     * @throws IOException
     * @throws URISyntaxException 
     * @throws UtilsException 
     * @throws UtilsException 
     */
	private void generateIndividualMiscFiles() 
	throws IOException, URISyntaxException, UtilsException {
		final Iterator<Site> siteIter = plan
		    .siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			Site site = siteIter.next();
			boolean isSink = false;
			if (this.plan.getRT().getRoot().equals(site)) {
				isSink = true;
			}
		    final String siteID = site.getID();
			final String nodeDir = targetDirName+"/mote" + siteID;
			copyInterfaceFile(INTERFACE_DO_TASK, nodeDir);

			generateMakefiles(nescOutputDir + nodeDir,
				"QueryPlan" + siteID, isSink);

	    	Template.instantiate(NESC_MISC_FILES_DIR + "/itoa.h",
					    nescOutputDir + nodeDir + "/itoa.h");
		    
		    if (this.useNodeController) {
		    	Template.instantiate(NESC_MISC_FILES_DIR + "/volumes-stm25p.xml",
					    nescOutputDir + nodeDir +"/volumes-stm25p.xml");	
		    	
		    	Template.instantiate(NESC_MISC_FILES_DIR + "/volumes-at45db.xml",
					    nescOutputDir + nodeDir +"/volumes-at45db.xml");
		    }
		    if (!this.useNodeController && isSink) {
		    	copySerialStarterFiles(nescOutputDir + nodeDir);
		    }
		    
		    if (this.usesCustomSeaLevelSensor) {
		    	copySeaLevelSensorFiles(nescOutputDir + nodeDir);
		    }
		}
	}

	private void copySeaLevelSensorFiles(String dir) throws IOException,
			URISyntaxException {
		Template.instantiate(NESC_MISC_FILES_DIR + 
				"/SeaLevelADC/Msp430Adc12.h",
			     dir +"/Msp430Adc12.h");
		Template.instantiate(NESC_MISC_FILES_DIR + 
				"/SeaLevelADC/Msp430AdcREADME.txt",
				dir + "/Msp430AdcREADME.txt");
		Template.instantiate(NESC_MISC_FILES_DIR + 
				"/SeaLevelADC/Msp430SeaLevelC.nc",
			    dir +"/Msp430SeaLevelC.nc");
		Template.instantiate(NESC_MISC_FILES_DIR + 
				"/SeaLevelADC/Msp430SeaLevelP.nc",
			    dir +"/Msp430SeaLevelP.nc");
		Template.instantiate(NESC_MISC_FILES_DIR + 
				"/SeaLevelADC/SeaLevelC.nc",
			    dir +"/SeaLevelC.nc");
	}

	private void generateCombinedMiscFiles() throws IOException, URISyntaxException {

			copyInterfaceFile(INTERFACE_DO_TASK, targetDirName +"/");

		    generateMakefiles(nescOutputDir+ targetDirName, "QueryPlan", false); 
		    	//! ok to hardcode to false?

	    	Template.instantiate(NESC_MISC_FILES_DIR + "/itoa.h",
					    nescOutputDir + targetDirName +"/itoa.h");

		    
		    if (this.useNodeController) {
		    	Template.instantiate(
					    NESC_MISC_FILES_DIR + "/volumes-stm25p.xml",
					    nescOutputDir + targetDirName +"/volumes-stm25p.xml");		    	
		    }
		    
	    	copySerialStarterFiles(nescOutputDir + targetDirName);
	    	
	    	if (this.usesCustomSeaLevelSensor) {
	    		copySeaLevelSensorFiles(nescOutputDir + targetDirName);
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
	private void generateTossimMiscFiles() throws IOException, URISyntaxException {

		copyInterfaceFile(INTERFACE_DO_TASK, targetDirName +"/");

	    generateMakefiles(nescOutputDir+ targetDirName, "QueryPlan", false);

    	Template.instantiate(
			    NESC_MISC_FILES_DIR + "/itoa.h",
			    nescOutputDir + targetDirName +"/itoa.h");

    	//Make sure that all nodes in QEP are run by Tossim
    	int maxSiteID = this.plan.getRT().getMaxSiteID();
    	
	    //By default run for one agenda evaluation
	    long duration = (this.plan.getAcquisitionInterval_ms()/1000) 
	    	* this.plan.getBufferingFactor();
	    
	    HashMap<String, String> replacements = new HashMap<String, String>();
		replacements.put("__SIMULATION_DURATION_SECS__", ""+duration);
		replacements.put("__NUM_NODES__", ""+(maxSiteID+1));
		Template.instantiate(
	    		NESC_MISC_FILES_DIR + "/runTossim.py",
	    		nescOutputDir + targetDirName +"/runTossim.py", replacements);

		Template.instantiate(
			    NESC_MISC_FILES_DIR + "/meyer-light.txt",
			    nescOutputDir + targetDirName +"/meyer-light.txt");
		
		Template.instantiate(
			    NESC_MISC_FILES_DIR + "/RandomSensorC.nc",
			    nescOutputDir + targetDirName +"/RandomSensorC.nc");
		
    	copySerialStarterFiles(nescOutputDir + targetDirName);
    	
    	if (this.usesCustomSeaLevelSensor) {
    		copySeaLevelSensorFiles(nescOutputDir + targetDirName);
    	}
	}


	/**
	 * Generates Makefile and MakeRules files in the given directory.
	 * @param dir The directory where the files are to be created.
	 * @param mainConfigName The name of main nesC configuration.
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	private void generateMakefiles(final String dir, String mainConfigName, boolean isSink) throws IOException, URISyntaxException {
		// Makefile
		HashMap<String, String> replacements = new HashMap<String, String>();
		replacements.put("__MAIN_CONFIG_NAME__", mainConfigName);
		
		if (this.useNodeController) {
			replacements.put("__DELUGE__","BOOTLOADER=tosboot");			
		} else {
			replacements.put("__DELUGE__","");
		}
		
		if (this.enablePrintf && target == CodeGenTarget.TELOSB_T2) {
			replacements.put("__PRINTF__", "CFLAGS += -I$(TOSDIR)/lib/printf");
		} else {
			replacements.put("__PRINTF__","");			
		}
		
		if (this.useNodeController) {
			String cmdSrvDir = Utils.getResourcePath("etc/sncb/tools/nesC/CommandServer");
			replacements.put("__COMMAND_SERVER__", "CFLAGS += -I"+cmdSrvDir+"\n"+
					"CFLAGS += -DSNEE_IMAGE_ID=2\n" +
					"### For Drip:\n" +
					"CFLAGS += -I$(TOSDIR)/lib/net -I%T/lib/net/drip");
		} else {
			replacements.put("__COMMAND_SERVER__", "");
		}
		if (this.useNodeController && isSink) {
			replacements.put("__BASESTATION__", "CFLAGS +=-DCOMMAND_SERVER_BASESTATION");
		} else {
			replacements.put("__BASESTATION__", "");
		}
		
		Template.instantiate(NESC_MISC_FILES_DIR
			+ "/Makefile", dir
			+ "/Makefile", replacements);
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
		    copyMiscFiles();

		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage(), e);
		    throw new CodeGenerationException(e);
		} 
    }
}
