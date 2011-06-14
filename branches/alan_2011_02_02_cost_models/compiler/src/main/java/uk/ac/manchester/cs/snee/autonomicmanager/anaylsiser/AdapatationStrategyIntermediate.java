package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.Adapatation;
import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author stokesa6
 *class AnaylsiserStrategy2Rules encapsulates the rules designed to calculate 
 *what possible changes the autonomic manager can do to adjust for a failure of a node
 */

public class AdapatationStrategyIntermediate
{
  private AutonomicManager manager;
  private boolean spacePinned;
  private boolean timePinned;
  private SensorNetworkQueryPlan qep;
  private RT currentRoutingTree;
  private Topology wsnTopology;
  private IOT iot;
  private AgendaIOT agenda;
  private File outputFolder;
  private String sep = System.getProperty("file.separator");
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public AdapatationStrategyIntermediate(AutonomicManager autonomicManager, boolean spacePinned, boolean timePinned)
  {
    this.manager = autonomicManager;
    this.spacePinned = spacePinned;
    this.timePinned = timePinned;
    
  }
  
  public void initilise(QueryExecutionPlan qep) throws SchemaMetadataException 
  {
    this.qep = (SensorNetworkQueryPlan) qep;
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata)this.qep.getDLAF().getSource();
    this.wsnTopology =  sm.getTopology();
    outputFolder = manager.getOutputFolder();
    new AdapatationStrategyIntermediateUtils(this).outputTopologyAsDotFile(outputFolder, "/topology.dot");
    this.currentRoutingTree = this.qep.getRT();
    this.iot = this.qep.getIOT();
    this.agenda = this.qep.getAgendaIOT();
  }
  
  /**
   * calculates new QEP designed to adjust for the failed node. 
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws AgendaException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   * @throws SNCBException 
   * @throws CostParametersException 
   * @throws SNEEDataSourceException 
   * @throws TopologyReaderException 
   * @throws SourceMetadataException 
   * @throws UnsupportedAttributeTypeException 
   * @throws MetadataException 
   * @throws WhenSchedulerException 
   * @throws MalformedURLException 
   */
  public ArrayList<Adapatation> calculateNewQEP(int failedNodeID) throws OptimizationException, SchemaMetadataException, TypeMappingException, AgendaException, SNEEException, SNEEConfigurationException, MalformedURLException, WhenSchedulerException, MetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException
  { 
    new AdapatationStrategyIntermediateUtils(this).outputNewAgendaImage(outputFolder);
    //remove faield node
    removedDeadNodeData(agenda, failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    //create paf
    PAF paf = pinPhysicalOperators(agenda, iot, (Site) failedNode);
    
    //create new routing tree
    ArrayList<RT> routingTrees = createNewRoutingTrees(agenda, iot, failedNodeID, paf);
    //create store for all adapatations
    ArrayList<Adapatation> totalAdapatations = new ArrayList<Adapatation>();
    Iterator<RT> routeIterator = routingTrees.iterator();
    while(routeIterator.hasNext())
    {
      //set up current objects
      RT routingTree =  routeIterator.next();
      Adapatation currentAdapatation = new Adapatation(qep);
      
      //run fragment paf though where scheduler.
      InstanceWhereSchedular instanceWhere = new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), outputFolder.toString());
      IOT newIOT = instanceWhere.getIOT();
      //analysis newIOT for interesting nodes
      checkIOT(newIOT, iot, currentAdapatation);
        
      //TODO run new iot though when scheduler and locate changes
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //check if agenda agrees to constraints.
      boolean success = checkAgendas(agenda, newAgenda, currentAdapatation);
      //check if new plan agrees with time pinning
      if(success)
      {
        DAF daf = new IOTUtils(newIOT, qep.getCostParameters()).convertToDAF();
        SensorNetworkQueryPlan newQep = new SensorNetworkQueryPlan(qep.getDLAF(), routingTree, daf, newIOT, newAgenda, qep.getID());
        currentAdapatation.setNewQep(newQep);
        totalAdapatations.add(currentAdapatation);
      }
    }
    return totalAdapatations;
  }

  private boolean checkAgendas(AgendaIOT agenda2, AgendaIOT newAgenda,
      Adapatation currentAdapatation)
  {
    // TODO Auto-generated method stub
    return false;
  }

  private void checkIOT(IOT newIOT, IOT iot2, Adapatation currentAdapatation)
  {
    // TODO Auto-generated method stub
    
  }

  private AgendaIOT doSNWhenScheduling(IOT newIOT, QoSExpectations qos,
                                       String id, CostParameters costParameters)
  throws SNEEConfigurationException, SNEEException, SchemaMetadataException,
         OptimizationException, WhenSchedulerException, MalformedURLException, TypeMappingException, MetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException 
  {
      boolean decreaseBetaForValidAlpha = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.WHEN_SCHED_DECREASE_BETA_FOR_VALID_ALPHA);
      boolean useNetworkController = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
      boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
      MetadataManager metadata = new MetadataManager(qep.getSNCB());
      WhenScheduler whenSched = new WhenScheduler(decreaseBetaForValidAlpha,
          allowDiscontinuousSensing, metadata, useNetworkController);
      AgendaIOT agenda = whenSched.doWhenScheduling(iot, qos, qep.getID(), qep.getCostParameters());
      if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) 
      {
       // new AgendaIOTUtils(agenda, iot, true).generateImage();
      }   
      return agenda;
  }

  /**
   * creates a new routeing tree for the where scheduler
   * @param agenda2
   * @param iot2
   * @param failedNodeID
   * @param paf 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private ArrayList<RT> createNewRoutingTrees(AgendaIOT agenda2, IOT iot2,
      int failedNodeID, PAF paf) throws NumberFormatException, SNEEConfigurationException
  {
    ArrayList<RT> routes = new ArrayList<RT>();
    Router router = new Router();
    RT route = router.doRouting(paf, "");
    routes.add(route);
    new AdapatationStrategyIntermediateUtils(this).outputRouteAsDotFile(outputFolder, "newRoute" + routes.size(), route);
    return routes;
  }

  /**
   * creates a fragment of a physical operator tree, this fragment encapsulates the failed nodes operators.
   * @param agenda2
   * @param iot
   * @param failedNode
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   * @throws OptimizationException 
   */
  private PAF pinPhysicalOperators(AgendaIOT agenda2, IOT iot, Site failedNode) throws SNEEException, SchemaMetadataException, SNEEConfigurationException, OptimizationException
  {
    //get paf 
    PAF paf = iot.getPAF();
    //get iterator for IOT without exchanges
    Iterator<InstanceOperator> iotInstanceOperatorIterator = iot.treeIterator(TraversalOrder.POST_ORDER, false);
    while(iotInstanceOperatorIterator.hasNext())
    {
      InstanceOperator instanceOperator = iotInstanceOperatorIterator.next();
      if(!instanceOperator.getSite().getID().equals(failedNode.getID()))
      {
        SensornetOperator physicalOperator = instanceOperator.getSensornetOperator();
        SensornetOperatorImpl physicalOperatorImpl = (SensornetOperatorImpl) physicalOperator;
        physicalOperatorImpl.setIsPinned(true);
        physicalOperatorImpl.addSiteToPinnedList(instanceOperator.getSite().getID());
      }
    }
    
    Iterator<SensornetOperator> pafIterator = paf.operatorIterator(TraversalOrder.POST_ORDER);
    while(pafIterator.hasNext())
    {
      SensornetOperator physicalOperator = pafIterator.next();
      if(physicalOperator instanceof SensornetExchangeOperator)
      {
        paf.getOperatorTree().removeNode(physicalOperator);
      }
    }
    return paf;
  }
  
  /**
   * removes the failed node from all dependent objects
   * @param newAgenda
 * @param iot 
   * @param wsnTopology2
   * @param newIOT
   * @param failedNodeID
   * @throws OptimizationException
   */
  private void removedDeadNodeData(AgendaIOT newAgenda, int failedNodeID) 
  throws OptimizationException
  {
    //remove dead node from new agenda, topology, new iot(so that adjustments can be made)
    newAgenda.removeNodeFromAgenda(failedNodeID);
    new AdapatationStrategyIntermediateUtils(this).outputTopologyAsDotFile(outputFolder, "/topologyAfterNodeLoss.dot");
  }

  public Topology getWsnTopology()
  {
    return wsnTopology;
  }

  public AgendaIOT getAgenda()
  {
    return agenda;
  }
  
  public IOT getIOT()
  {
    return iot;
  }
}
