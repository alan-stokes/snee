package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;

import java.io.File;
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
  private RT routingTree;
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
    this.routingTree = this.qep.getRT();
    this.iot = this.qep.getIOT();
    this.agenda = this.qep.getAgenda().getAgendaIOT();
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
   */
  public SensorNetworkQueryPlan calculateNewQEP(int failedNodeID) throws OptimizationException, SchemaMetadataException, TypeMappingException, AgendaException, SNEEException, SNEEConfigurationException
  { 
    new AdapatationStrategyIntermediateUtils(this).outputNewAgendaImage(outputFolder);
    //remove faield node
    removedDeadNodeData(agenda, failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    //create paf
    PAF paf = pinPhysicalOperators(agenda, iot, (Site) failedNode);
    //create new routing tree
    createNewRoutingTree(agenda, iot, failedNodeID, paf);
    //run fragment paf though where scheduler.
    InstanceWhereSchedular instanceWhere = new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), outputFolder.toString());
    IOT newIOT = instanceWhere.getIOT();
    //TODO run new iot though when scheduler and locate changes
   // AgendaIOT newAgenda = doSNWhenScheduling(iot, , , costParams);
    
    return qep;
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
  private void createNewRoutingTree(AgendaIOT agenda2, IOT iot2,
      int failedNodeID, PAF paf) throws NumberFormatException, SNEEConfigurationException
  {
    Router router = new Router();
    routingTree = router.doRouting(paf, "");
    new AdapatationStrategyIntermediateUtils(this).outputRouteAsDotFile(outputFolder, "newRoute");
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
  
  public RT getRT()
  {
    return routingTree;
  }
}
