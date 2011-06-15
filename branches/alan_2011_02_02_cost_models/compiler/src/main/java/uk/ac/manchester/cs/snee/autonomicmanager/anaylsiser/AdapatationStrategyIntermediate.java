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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

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
  
  public void initilise(QueryExecutionPlan oldQep) throws SchemaMetadataException 
  {
    this.qep = (SensorNetworkQueryPlan) oldQep;
    
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
  public ArrayList<Adapatation> calculateNewQEP(ArrayList<String> failedNodes) throws OptimizationException, SchemaMetadataException, TypeMappingException, AgendaException, SNEEException, SNEEConfigurationException, MalformedURLException, WhenSchedulerException, MetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException
  { 
    System.out.println("Running fake Adapatation ");
    //clone the iot so that any changes do not affect comparisons.
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    IOT oldIOT = cloner.deepClone(iot);
    
    //remove faield node
    removedDeadNodeData(agenda, failedNodes);
    //create paf
    PAF paf = pinPhysicalOperators(agenda, iot, failedNodes);
    
    //create new routing tree
    ArrayList<RT> routingTrees = createNewRoutingTrees(agenda, iot, failedNodes, paf);
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
        
      //run new iot though when scheduler and locate changes
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //output new and old agendas
      new AdapatationStrategyIntermediateUtils(this).outputAgendas(newAgenda, qep.getAgendaIOT(), outputFolder);
      //analysis newIOT for interesting nodes
      checkIOT(newIOT, oldIOT, failedNodes, currentAdapatation);
      //check if agenda agrees to constraints.
      boolean success = checkAgendas(agenda, newAgenda, currentAdapatation);
      //check if new plan agrees with time pinning
      if(success)
      {
        DAF daf = new IOTUtils(newIOT, qep.getCostParameters()).getDAF();
        SensorNetworkQueryPlan newQep = new SensorNetworkQueryPlan(qep.getDLAF(), routingTree, daf, newIOT, newAgenda, qep.getID());
        currentAdapatation.setNewQep(newQep);
        totalAdapatations.add(currentAdapatation);
      }
    }
    return totalAdapatations;
  }

  /**
   * chekcs agendas for time pinning and temperoral adjustments.
   * @param agenda2
   * @param newAgenda
   * @param currentAdapatation
   * @return
   */
  private boolean checkAgendas(AgendaIOT oldAgenda, AgendaIOT newAgenda,
      Adapatation currentAdapatation)
  {
    checkForTemporalChnagedNodes(newAgenda, oldAgenda, currentAdapatation);
    if(timePinned)
      if(currentAdapatation.getTemporalChangesSize() == 0)
        return true;
      else
        return false;
    else
      return true;
  }

  /**
   * checks between old and new agendas and locates nodes whos fragments need a temporal adjustment.
   * @param newAgenda
   * @param oldAgenda
   * @param currentAdapatation
   */
  private void checkForTemporalChnagedNodes(AgendaIOT newAgenda,
      AgendaIOT oldAgenda, Adapatation currentAdapatation)
  {
    // TODO Auto-generated method stub
    
  }

  /**
   * checks iots for the different types of adapatations on nodes which are required
   * @param newIOT
   * @param failedNodes 
   * @param iot2
   * @param currentAdapatation
   */
  private void checkIOT(IOT newIOT, IOT oldIOT, ArrayList<String> failedNodes, Adapatation currentAdapatation)
  {
    //check reprogrammed nodes
    checkForReProgrammedNodes(newIOT, oldIOT, currentAdapatation);
    checkForReDirectionNodes(newIOT, oldIOT, currentAdapatation);
    checkForDeactivatedNodes(newIOT, oldIOT, failedNodes, currentAdapatation);
  }

  /**
   * checks iots for nodes which have operators in the old IOT, but have none in the new iot
   * @param newIOT
   * @param oldIOT
   * @param failedNodes.contains(o) 
   * @param currentAdapatation
   */
  private void checkForDeactivatedNodes(IOT newIOT, IOT oldIOT,
      ArrayList<String> failedNodes, Adapatation ad)
  {
    RT rt = oldIOT.getRT();
    Iterator<Site> siteIterator = rt.siteIterator(TraversalOrder.PRE_ORDER);
    //get rid of root site (no exchanges to work with)
    siteIterator.next();
    //go though each site, looking to see if destination sites are the same for exchanges.
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      ArrayList<InstanceOperator> instanceOperatorsNew = newIOT.getOpInstances(site, TraversalOrder.PRE_ORDER, true);
      if(instanceOperatorsNew.size() == 0 && !failedNodes.contains(site.getID()))
      {
        ad.addDeactivatedSite(site);
      }
    }
  }

  /**
   * check all sites in new IOT for sites in the old IOT where they communicate with different sites.
   * @param newIOT
   * @param oldIOT
   * @param ad
   */
  private void checkForReDirectionNodes(IOT newIOT, IOT oldIOT,
      Adapatation ad)
  {
    RT rt = newIOT.getRT();
    Iterator<Site> siteIterator = rt.siteIterator(TraversalOrder.PRE_ORDER);
    //get rid of root site (no exchanges to work with)
    siteIterator.next();
    //go though each site, looking to see if destination sites are the same for exchanges.
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      ArrayList<InstanceOperator> instanceOperatorsNew = newIOT.getOpInstances(site, TraversalOrder.PRE_ORDER, true);
      ArrayList<InstanceOperator> instanceOperatorsOld = oldIOT.getOpInstances(site, TraversalOrder.PRE_ORDER, true);
      InstanceExchangePart exchangeNew = (InstanceExchangePart) instanceOperatorsNew.get(0);
      InstanceExchangePart exchangeOld = null;
      if(instanceOperatorsOld.size() == 0)
      {}
      else
      {
        exchangeOld = (InstanceExchangePart) instanceOperatorsOld.get(0);
        if(!exchangeNew.getNext().getSite().getID().equals(exchangeOld.getNext().getSite().getID()))
        {
          ad.addRedirectedSite(site);
        }
      }
    }
  }
  
  /**
   * checks for nodes which have need to be reprogrammed to go from one IOT to the other
   * @param newIOT
   * @param oldIOT
   * @param currentAdapatation
   */
  private void checkForReProgrammedNodes(IOT newIOT, IOT oldIOT,
      Adapatation ad)
  {
    RT rt = newIOT.getRT();
    Iterator<Site> siteIterator = rt.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      ArrayList<SensornetOperator> physicalOpsNew = new ArrayList<SensornetOperator>();
      ArrayList<SensornetOperator> physicalOpsOld = new ArrayList<SensornetOperator>();
      ArrayList<InstanceOperator> instanceOperatorsNew = newIOT.getOpInstances(site, true);
      ArrayList<InstanceOperator> instanceOperatorsOld = oldIOT.getOpInstances(site, true);
      Iterator<InstanceOperator> newInsOpIterator = instanceOperatorsNew.iterator();
      Iterator<InstanceOperator> oldInsOpIterator = instanceOperatorsOld.iterator();
      while(newInsOpIterator.hasNext())
      {
        physicalOpsNew.add(newInsOpIterator.next().getSensornetOperator());
      }
      while(oldInsOpIterator.hasNext())
      {
        physicalOpsOld.add(oldInsOpIterator.next().getSensornetOperator());
      }
      Iterator<SensornetOperator> newSenOpIterator = physicalOpsNew.iterator();
      Iterator<SensornetOperator> oldSenOpIterator = physicalOpsOld.iterator();
      if(physicalOpsNew.size() != physicalOpsOld.size())
      {
        ad.addReprogrammedSite(site); 
      }
      else
      {
        boolean notSame = false;
        while(newSenOpIterator.hasNext() && notSame)
        {
          SensornetOperator newSenOp = newSenOpIterator.next();
          SensornetOperator oldSenOp = oldSenOpIterator.next();
          if(!newSenOp.getID().equals(oldSenOp.getID()))
          {
            ad.addReprogrammedSite(site); 
            notSame = true;
          }
        }
      }
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
   */
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
   * @param failedNodes
   * @param paf 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private ArrayList<RT> createNewRoutingTrees(AgendaIOT agenda2, IOT iot2,
      ArrayList<String> failedNodes, PAF paf) throws NumberFormatException, SNEEConfigurationException
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
   * @param failedNodes
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   * @throws OptimizationException 
   */
  private PAF pinPhysicalOperators(AgendaIOT agenda2, IOT iot, ArrayList<String> failedNodes) throws SNEEException, SchemaMetadataException, SNEEConfigurationException, OptimizationException
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
      boolean locatedOnAFailedNode = false;
      Iterator<String> failedNodeIterator = failedNodes.iterator();
      while(failedNodeIterator.hasNext() && !locatedOnAFailedNode)
      {
        if(instanceOperator.getSite().getID().equals(failedNodeIterator.next()))
          locatedOnAFailedNode = true;
      }
      if(!locatedOnAFailedNode)
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
    return paf;
  }
  
  /**
   * removes the failed node from all dependent objects
   * @param newAgenda
   * @param iot 
   * @param wsnTopology2
   * @param newIOT
   * @param failedNodes
   * @throws OptimizationException
   */
  private void removedDeadNodeData(AgendaIOT newAgenda, ArrayList<String> failedNodes) 
  throws OptimizationException
  {
    //remove dead node from new agenda, topology, new iot(so that adjustments can be made)
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    while(failedNodeIterator.hasNext())
      newAgenda.removeNodeFromAgenda(Integer.parseInt(failedNodeIterator.next()));
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
