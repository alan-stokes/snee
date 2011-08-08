package uk.ac.manchester.cs.snee.manager;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public abstract class FrameWorkAbstract
{
  protected SensorNetworkQueryPlan qep;
  protected AutonomicManager manager;
  protected SourceMetadataAbstract _metadata;
  protected File outputFolder;
  protected String sep = System.getProperty("file.separator");
  
  /**
   * bog standard constructor
   * @param manager
   */
  public FrameWorkAbstract(AutonomicManager manager, SourceMetadataAbstract _metadata)
  {
    this.manager = manager;
    this._metadata = _metadata;
  }
  /**
   * checks that the framework can adapt to one failed node
   * @param failedNode
   * @return
   */
  public abstract boolean canAdapt(String failedNode);
  /**
   * checks that the framework can adapt to all the failed nodes
   * @param failedNodes
   * @return
   */
  public abstract boolean canAdaptToAll(ArrayList<String> failedNodes);
  /**
   * used to set up a framework
   * @param oldQep
   * @param noTrees
   */
  public abstract void initilise(QueryExecutionPlan oldQep, Integer noTrees)
  throws SchemaMetadataException ;
  /**
   * calculates a set of adpatations which will produce new QEPs which respond to the 
   * failed node. 
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   */
  public abstract List<Adapatation> adapt(ArrayList<String> failedNodes)  
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, 
  SNEEException, SNEEConfigurationException, 
  MalformedURLException, MetadataException, 
  UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, 
  SNEEDataSourceException, CostParametersException, 
  SNCBException, NumberFormatException, SNEECompilerException;
  
  
  /**
   * helper method to get topology from the qep
   * @return topology
   */
  public Topology getWsnTopology()
  {
//    qep.getMetaData().getOutputAttributes().get(0).getExtentName()_metadata.getSource(qep.getMetaData())
    Set<SourceMetadataAbstract> sourceSets = qep.getDLAF().getSources();
    SensorNetworkSourceMetadata sm;
    if(sourceSets.size() == 1)
    {
      Iterator<SourceMetadataAbstract> sourceIterator = sourceSets.iterator();
      sm = (SensorNetworkSourceMetadata) sourceIterator.next();
      Topology network = sm.getTopology();
      return network;
    }
    else
    {
      System.out.println("error, more than 1 network");
      return null;
    }
  }
  
  
  protected boolean assessQEPsAgendas(IOT oldIOT, IOT newIOT, AgendaIOT oldAgenda, 
                                   AgendaIOT newAgenda, boolean timePinned,
                                   Adapatation adaptation, ArrayList<String> failedNodes,
                                   RT routingTree) 
  throws SchemaMetadataException, TypeMappingException
  {
  //analysis newIOT for interesting nodes
    checkIOT(newIOT, oldIOT, failedNodes, adaptation);
    //check if agenda agrees to constraints.
    boolean success = 
      checkAgendas(oldAgenda, newAgenda, newIOT, oldIOT, failedNodes, adaptation, timePinned);
    //check if new plan agrees with time pinning
    if(success)
    {//create new qep, add qep to list of adapatations.
      DAF daf = new IOTUtils(newIOT, qep.getCostParameters()).getDAF();
      SensorNetworkQueryPlan newQep 
      = new SensorNetworkQueryPlan(qep.getDLAF(), routingTree, daf, newIOT, newAgenda, qep.getID());
      adaptation.setNewQep(newQep);
    }
    return success;
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
      ArrayList<String> failedNodes, Adapatation currentAdapatation, boolean timePinned)
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


  /**
   * find all sites which are affected by this time adjustment
   * @param start
   * @param affectedSites
   * @param newAgenda
   * @param ad
   * @param adjust
   */
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

  /**
   * determines time differences between 2 communication tasks
   * @param newChild
   * @param orginal
   * @param parent
   * @param failedSite
   * @param startTime
   * @param duration
   * @param newAgenda
   * @param oldAgenda
   * @param adjust
   * @param ad
   * @return
   */
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
  
}
