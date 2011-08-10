package uk.ac.manchester.cs.snee.manager.failednode;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.FrameWorkAbstract;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.CandiateRouter;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

/**
 * @author stokesa6
 *class designed to encapsulate the partial framework of adapting just what needs to be adapted
 */

public class FailedNodeFrameWorkPartial extends FrameWorkAbstract
{
  private boolean spacePinned;
  private boolean timePinned;
  private IOT oldIOT;
  private AgendaIOT oldAgenda;
  private Integer numberOfRoutingTreesToWorkOn = 0;
  private static int choice;
  
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public FailedNodeFrameWorkPartial(AutonomicManager autonomicManager, 
                                    SourceMetadataAbstract _metadata, boolean spacePinned, 
                                    boolean timePinned)
  {
    super(autonomicManager, _metadata);
    this.spacePinned = spacePinned;
    this.timePinned = timePinned;
    this.timePinned = false; 
  }
  
  @Override
  public void initilise(QueryExecutionPlan oldQep, Integer numberOfTrees) 
  throws SchemaMetadataException 
  {
    this.qep = (SensorNetworkQueryPlan) oldQep;
    outputFolder = manager.getOutputFolder();
    new FailedNodeFrameWorkPartialUtils(this).outputTopologyAsDotFile(outputFolder, "/topology.dot");
    this.oldIOT = qep.getIOT();
    oldIOT.setID("OldIOT");
    this.oldAgenda = this.qep.getAgendaIOT();
    this.numberOfRoutingTreesToWorkOn = numberOfTrees;
  }
  
  @Override
  public List<Adapatation> adapt(ArrayList<String> failedNodes) 
  throws NumberFormatException, SNEEConfigurationException, SchemaMetadataException, 
  SNEECompilerException, MalformedURLException, SNEEException, OptimizationException,
  TypeMappingException, MetadataException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException 
  { 
    System.out.println("Running fake Adapatation "); 
    //setup collectors
    PAF paf = oldIOT.getPAF(); 
    ArrayList<RT> routingTrees = new ArrayList<RT>();
    ArrayList<String> disconnectedNodes = new ArrayList<String>();
    //create new routing tree
    routingTrees = createNewRoutingTrees(failedNodes, disconnectedNodes, paf, oldIOT.getRT(), outputFolder );

    //create store for all adapatations
    List<Adapatation> totalAdapatations = new ArrayList<Adapatation>();
    Iterator<RT> routeIterator = routingTrees.iterator();
    choice = 0;
    
    try
    {
      tryGoingThoughRoutes(routeIterator, failedNodes, disconnectedNodes, totalAdapatations);
    }
    catch(Exception e)
    {
      e.printStackTrace();
      tryGoingThoughRoutes(routeIterator, failedNodes, disconnectedNodes, totalAdapatations);
    }
    return totalAdapatations;
  }

  /**
   * if a route cant be calculated with the pinned sites, dynamically remove a site to allow adaptation.
   * @param oldIOT2
   * @param failedNodes
   * @param disconnectedNodes
   */
  private void chooseDisconnectedNode(IOT oldIOT2, ArrayList<String> failedNodes,
                                      ArrayList<String> disconnectedNodes)
  {
    // TODO Auto-generated method stub
    
  }

  /**
   * uses new routes to determine new QEP/adaptations
   * @param routeIterator @param failedNodes
   * @param disconnectedNodes @param totalAdapatations
   * @throws SNEEException @throws SchemaMetadataException
   * @throws OptimizationException @throws SNEEConfigurationException
   * @throws MalformedURLException @throws WhenSchedulerException
   * @throws TypeMappingException @throws MetadataException
   * @throws UnsupportedAttributeTypeException @throws SourceMetadataException
   * @throws TopologyReaderException @throws SNEEDataSourceException
   * @throws CostParametersException @throws SNCBException
   * @throws SNEECompilerException 
   */
  private void tryGoingThoughRoutes(Iterator<RT> routeIterator, ArrayList<String> failedNodes, 
                                    ArrayList<String> disconnectedNodes, 
                                    List<Adapatation> totalAdapatations)
  throws SNEEException, SchemaMetadataException, 
         OptimizationException, SNEEConfigurationException, 
         MalformedURLException, TypeMappingException, 
         MetadataException, UnsupportedAttributeTypeException, 
         SourceMetadataException, TopologyReaderException, 
         SNEEDataSourceException, CostParametersException, 
         SNCBException, SNEECompilerException
  {
    choice++;
    while(routeIterator.hasNext())
    {
      //set up current objects
      RT routingTree =  routeIterator.next();
      Adapatation currentAdapatation = new Adapatation(qep);
      File choiceFolder = new File(outputFolder.toString() + sep + "choice" + choice);
      choiceFolder.mkdir();
      //create pinned paf
      PAF paf = pinPhysicalOperators(oldIOT, failedNodes, disconnectedNodes);
      //run fragment paf though where scheduler.
      InstanceWhereSchedular instanceWhere = 
        new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), choiceFolder.toString());
      IOT newIOT = instanceWhere.getIOT();
      //run new iot though when scheduler and locate changes
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //output new and old agendas
      new FailedNodeFrameWorkPartialUtils(this).outputAgendas(newAgenda, qep.getAgendaIOT(), oldIOT, newIOT, choiceFolder);
      boolean success = assessQEPsAgendas(oldIOT, newIOT, oldAgenda, newAgenda, 
                                          timePinned, currentAdapatation, failedNodes, routingTree);
      if(success)
        totalAdapatations.add(currentAdapatation);
      choice++;
    }
    
  }

  

  /**
   * run when scheduling
   * @param newIOT
   * @param qos
   * @param id
   * @param costParameters
   * @return
   * @throws SNEEConfigurationException
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws OptimizationException
   * @throws WhenSchedulerException
   * @throws MalformedURLException
   * @throws TypeMappingException
   * @throws MetadataException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws TopologyReaderException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws SNEECompilerException 
   */
  private AgendaIOT doSNWhenScheduling(IOT newIOT, QoSExpectations qos,
                                       String id, CostParameters costParameters)
  throws SNEEConfigurationException, SNEEException, 
  SchemaMetadataException, OptimizationException, 
  MalformedURLException, TypeMappingException, 
  MetadataException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, 
  SNEEDataSourceException, CostParametersException, 
  SNCBException, SNEECompilerException 
  {
      boolean useNetworkController = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
      boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
      MetadataManager metadata = new MetadataManager(qep.getSNCB());
      WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, metadata, useNetworkController);
      AgendaIOT agenda;
      try
      {
        agenda = whenSched.doWhenScheduling(newIOT, qos, qep.getID(), qep.getCostParameters());
      }
      catch (WhenSchedulerException e)
      {
        throw new SNEECompilerException(e);
      }  
      agenda.setID("new Agenda");
      this.oldAgenda.setID("old Agenda");
      return agenda;
  }

  /**
   * creates a new routeing tree for the where scheduler
   * @param agenda2
   * @param iot2
   * @param failedNodes
   * @param disconnectedNodes 
   * @param paf 
   * @param outputFolder2 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws SchemaMetadataException 
   */
  private ArrayList<RT> createNewRoutingTrees(ArrayList<String> failedNodes, 
      ArrayList<String> disconnectedNodes, PAF paf, RT oldRoutingTree, File outputFolder) 
  throws 
  NumberFormatException, SNEEConfigurationException, 
  SchemaMetadataException, SNEECompilerException
  {
    ArrayList<RT> routes = new ArrayList<RT>();
    Topology network = this.getWsnTopology();
    CandiateRouter router = new CandiateRouter(network, outputFolder);
    while(routes.size() == 0)
    {  
      routes = router.findAllRoutes(oldRoutingTree, failedNodes, "", numberOfRoutingTreesToWorkOn);
      if(routes.size() == 0)
      {
        chooseDisconnectedNode(oldIOT, failedNodes, disconnectedNodes);
      }
    }
    return routes;
  }

  /**
   * creates a fragment of a physical operator tree, this fragment encapsulates the failed nodes operators.
   * @param agenda2
   * @param iot
   * @param failedNodes
   * @param disconnectedNodes 
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   * @throws OptimizationException 
   */
  private PAF pinPhysicalOperators(IOT iot, ArrayList<String> failedNodes, 
                                   ArrayList<String> disconnectedNodes) 
  throws SNEEException, 
         SchemaMetadataException, 
         SNEEConfigurationException, 
         OptimizationException
  {
    //get paf 
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    PAF paf = cloner.deepClone(iot.getPAF());
    //get iterator for IOT without exchanges
    Iterator<InstanceOperator> iotInstanceOperatorIterator = iot.treeIterator(TraversalOrder.POST_ORDER, false);
    ArrayList<SensornetOperatorImpl> opsOnFailedNode = new ArrayList<SensornetOperatorImpl>();
    while(iotInstanceOperatorIterator.hasNext())
    {
      InstanceOperator instanceOperator = iotInstanceOperatorIterator.next();
      SensornetOperator physicalOperator = instanceOperator.getSensornetOperator();
      SensornetOperatorImpl physicalOperatorImpl = (SensornetOperatorImpl) physicalOperator;
      if(!failedNodes.contains(instanceOperator.getSite().getID()) && 
         !disconnectedNodes.contains(instanceOperator.getSite().getID()))
      {
        ((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())).setIsPinned(true);
        ((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())).addSiteToPinnedList(instanceOperator.getSite().getID());
      }
      else
      {
        if(!(physicalOperator instanceof SensornetAcquireOperator))
          opsOnFailedNode.add(((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())));
      }
    }
    //remove total pinning on operators located on failed node
    Iterator<SensornetOperatorImpl> failedNodeOpIterator = opsOnFailedNode.iterator();
    while(failedNodeOpIterator.hasNext())
    {
      SensornetOperatorImpl physicalOperatorImpl = ((SensornetOperatorImpl) paf.getOperatorTree().getNode(failedNodeOpIterator.next().getID()));
      physicalOperatorImpl.setTotallyPinned(false);
    }
    
    //remove exchange operators (does not exist in a paf)
    Iterator<SensornetOperator> pafIterator = paf.operatorIterator(TraversalOrder.POST_ORDER);
    while(pafIterator.hasNext())
    {
      SensornetOperator physicalOperator = pafIterator.next();
      if(physicalOperator instanceof SensornetExchangeOperator)
      {
        paf.getOperatorTree().removeNode(physicalOperator);
      }
    }
    paf.setID("PinnedPAF");
    return paf;
  }

  public AgendaIOT getOldAgenda()
  {
    return oldAgenda;
  }
  
  public IOT getOldIOT()
  {
    return oldIOT;
  }

  @Override
  public boolean canAdapt(String failedNode)
  {
    return true;
  }

  @Override
  public boolean canAdaptToAll(ArrayList<String> failedNodes)
  {
    return true;
  }
}
