package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.Adapatation;
import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.autonomicmanager.TemporalAdjustment;
import uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router.CandiateRouter;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
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
  private IOT oldIOT;
  private AgendaIOT agenda;
  private File outputFolder;
  private String sep = System.getProperty("file.separator");
  private Integer numberOfRoutingTreesToWorkOn = 0;
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public AdapatationStrategyIntermediate(AutonomicManager autonomicManager, boolean spacePinned, boolean timePinned)
  {
    this.manager = autonomicManager;
    this.spacePinned = spacePinned;
    this.timePinned = timePinned;
    this.timePinned = false;
    
  }
  
  public void initilise(QueryExecutionPlan oldQep, Integer numberOfTrees) throws SchemaMetadataException 
  {
    this.qep = (SensorNetworkQueryPlan) oldQep;
    outputFolder = manager.getOutputFolder();
    new AdapatationStrategyIntermediateUtils(this).outputTopologyAsDotFile(outputFolder, "/topology.dot");
    this.oldIOT = qep.getIOT();
    oldIOT.setID("OldIOT");
    this.agenda = this.qep.getAgendaIOT();
    this.numberOfRoutingTreesToWorkOn = numberOfTrees;
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
    //generate topology file
    new AdapatationStrategyIntermediateUtils(this).outputTopologyAsDotFile(outputFolder, sep + "topologyAfterNodeLoss.dot");
    //setup collectors
    PAF paf = oldIOT.getPAF(); 
    ArrayList<RT> routingTrees = new ArrayList<RT>();
    ArrayList<String> disconnectedNodes = new ArrayList<String>();
    //create new routing tree
    routingTrees = createNewRoutingTrees(failedNodes, disconnectedNodes, paf, oldIOT.getRT());

    //create store for all adapatations
    ArrayList<Adapatation> totalAdapatations = new ArrayList<Adapatation>();
    Iterator<RT> routeIterator = routingTrees.iterator();
    
    try
    {
      tryGoingThoughRoutes(routeIterator, failedNodes, disconnectedNodes, totalAdapatations);
    }
    catch(Exception e)
    {
      tryGoingThoughRoutes(routeIterator, failedNodes, disconnectedNodes, totalAdapatations);
    }
    return totalAdapatations;
  }

  private void chooseDisconnectedNode(IOT oldIOT2, ArrayList<String> failedNodes,
                                      ArrayList<String> disconnectedNodes)
  {
    // TODO Auto-generated method stub
    
  }

  private void tryGoingThoughRoutes(Iterator<RT> routeIterator, ArrayList<String> failedNodes, 
                                    ArrayList<String> disconnectedNodes, 
                                    ArrayList<Adapatation> totalAdapatations)
  throws SNEEException, 
         SchemaMetadataException, 
         OptimizationException, 
         SNEEConfigurationException, 
         MalformedURLException, 
         WhenSchedulerException, 
         TypeMappingException, 
         MetadataException, 
         UnsupportedAttributeTypeException, 
         SourceMetadataException, 
         TopologyReaderException, 
         SNEEDataSourceException,
         CostParametersException, 
         SNCBException
  {
    while(routeIterator.hasNext())
    {
      //set up current objects
      RT routingTree =  routeIterator.next();
      Adapatation currentAdapatation = new Adapatation(qep);
      
      //create pinned paf
      PAF paf = pinPhysicalOperators(oldIOT, failedNodes, disconnectedNodes);
      //run fragment paf though where scheduler.
      InstanceWhereSchedular instanceWhere = new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), outputFolder.toString());
      IOT newIOT = instanceWhere.getIOT();
      //run new iot though when scheduler and locate changes
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //output new and old agendas
      new AdapatationStrategyIntermediateUtils(this).outputAgendas(newAgenda, qep.getAgendaIOT(), oldIOT, newIOT, outputFolder);
      //analysis newIOT for interesting nodes
      checkIOT(newIOT, oldIOT, failedNodes, currentAdapatation);
      //check if agenda agrees to constraints.
      boolean success = checkAgendas(agenda, newAgenda, newIOT, oldIOT, failedNodes, currentAdapatation);
      //check if new plan agrees with time pinning
      if(success)
      {//create new qep, add qep to list of adapatations.
        DAF daf = new IOTUtils(newIOT, qep.getCostParameters()).getDAF();
        SensorNetworkQueryPlan newQep = new SensorNetworkQueryPlan(qep.getDLAF(), routingTree, daf, newIOT, newAgenda, qep.getID());
        currentAdapatation.setNewQep(newQep);
        totalAdapatations.add(currentAdapatation);
      }
    }
    
  }

  /**
   * chekcs agendas for time pinning and temperoral adjustments.
   * @param agenda2
   * @param newAgenda
   * @param failedNodes 
   * @param currentAdapatation
   * @return
   */
  private boolean checkAgendas(AgendaIOT oldAgenda, AgendaIOT newAgenda, IOT newIOT, IOT oldIOT,
      ArrayList<String> failedNodes, Adapatation currentAdapatation)
  {
    checkForTemporalChangedNodes(newAgenda, oldAgenda, newIOT, oldIOT, failedNodes, currentAdapatation);
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
   * @param failedNodes 
   * @param currentAdapatation
   */
  private void checkForTemporalChangedNodes(AgendaIOT newAgenda,
      AgendaIOT oldAgenda,  IOT newIOT, IOT oldIOT, ArrayList<String> failedNodes, 
      Adapatation ad)
  {
    Iterator<String> failedNodesIterator = failedNodes.iterator();
    while(failedNodesIterator.hasNext())
    {
      Site failedSite  = (Site) oldIOT.getNode(failedNodesIterator.next());
      ArrayList<Node> children = oldIOT.getInputSites(failedSite);
      Iterator<Node> childrenIterator = children.iterator();
      ArrayList<Site> affectedSites = new ArrayList<Site>();
      long startTime = 0;
      long duration = 0;
      boolean reprogrammed = true;
      while(childrenIterator.hasNext() && reprogrammed)
      {
        Node child = newIOT.getNode(childrenIterator.next().getID());
        Node orginal = child;
        Node lastChild = null;
        while(reprogrammed)
        {
          Node nextChild = newAgenda.getTransmissionTask(child).getDestNode();
          if (ad.reprogrammingContains((Site) nextChild))
          {
            lastChild = child;
            child = nextChild;
          }
          else
          {
            lastChild = child;
            TemporalAdjustment adjust = new TemporalAdjustment();
            boolean changed = sortOutTiming(lastChild, orginal, nextChild, (Node) failedSite, startTime, 
                          duration, newAgenda, oldAgenda, adjust, ad);
            if(changed)
            {
              findAffectedSites(nextChild, affectedSites, newAgenda, ad, adjust);
              adjust.setAffectedSites(affectedSites);
              ad.addTemporalSite(adjust);
            }
            reprogrammed = false;           
          } 
        }
      }
    } 
  }


  private void findAffectedSites(Node start, ArrayList<Site> affectedSites, AgendaIOT newAgenda,
		                         Adapatation ad, TemporalAdjustment adjust)
  {
    affectedSites.add((Site) start);
    Task comm = newAgenda.getTransmissionTask(start); 
    ArrayList<Node> sites =  newAgenda.sitesWithTransmissionTasksAfterTime(comm.getStartTime());
    Iterator<Node> siteIterator = sites.iterator();
    while(siteIterator.hasNext())
    {
      start = siteIterator.next();
      ArrayList<Site> allAffectedSites = ad.getSitesAffectedByAllTemporalChanges();
      if(allAffectedSites.contains(start))
      {
    	  TemporalAdjustment otherAdjust = ad.getAdjustmentContainingSite((Site) start);
    	  if(otherAdjust.getAdjustmentDuration() < adjust.getAdjustmentDuration())
    	  {
    	    affectedSites.add((Site) start);
    	    otherAdjust.removeSiteFromAffectedSites((Site) start);
    	  }
      }
      else
      {
        affectedSites.add((Site) start); 
      }
    }    
  }

  private boolean sortOutTiming(Node newChild, Node orginal, Node parent, 
      Node failedSite, Long startTime, Long duration, AgendaIOT newAgenda, 
      AgendaIOT oldAgenda, TemporalAdjustment adjust, Adapatation ad)
  {
    Task commTask = newAgenda.getCommunicationTaskBetween(newChild, parent);
    Task commTimeOld = oldAgenda.getCommunicationTaskBetween(failedSite, parent);
    //if failed site not got a direct communication between itself and the parent, look for a parent of the failed node which does
    while(commTimeOld == null)
    {
      failedSite = oldAgenda.getTransmissionTask(failedSite).getDestNode();
      commTimeOld = oldAgenda.getCommunicationTaskBetween(failedSite, parent);
    }
    long commStartTime = commTask.getStartTime();
    long commOldStartTime = commTimeOld.getStartTime();
    if(commStartTime != commOldStartTime)
    {
      Long difference = new Long(commStartTime - commOldStartTime);
      ArrayList<Long> differeneces = ad.getTemporalDifferences();
      if(difference > 0 && !differeneces.contains(difference))
      {
        if(commStartTime > startTime)
        {
          startTime = commOldStartTime;
          duration = difference;
          adjust.setAdjustmentPosition(startTime);
          adjust.setAdjustmentDuration(duration);
          return true;
        }
      }
    }
    return false;
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
      AgendaIOT agenda = whenSched.doWhenScheduling(newIOT, qos, qep.getID(), qep.getCostParameters());  
      agenda.setID("new Agenda");
      this.agenda.setID("old Agenda");
      return agenda;
  }

  /**
   * creates a new routeing tree for the where scheduler
   * @param agenda2
   * @param iot2
   * @param failedNodes
   * @param disconnectedNodes 
   * @param paf 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private ArrayList<RT> createNewRoutingTrees(ArrayList<String> failedNodes, ArrayList<String> disconnectedNodes, PAF paf, RT oldRoutingTree) throws NumberFormatException, SNEEConfigurationException
  {
    ArrayList<RT> routes = new ArrayList<RT>();
    //Router router = new Router();
    CandiateRouter router = new CandiateRouter();
    while(routes.size() == 0)
    {
      //RT route = router.doRouting(paf, "");
      //route.setID("newRoute" + routes.size()+1);
      //new AdapatationStrategyIntermediateUtils(this).outputRouteAsDotFile(outputFolder, "newRoute" + route.getID(), route);
      //routes.add(route);
      
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

  public Topology getWsnTopology()
  {
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) 
    qep.getDLAF().getSource();
    Topology network = sm.getTopology();
    return network;
  }

  public AgendaIOT getAgenda()
  {
    return agenda;
  }
  
  public IOT getOldIOT()
  {
    return oldIOT;
  }
}
