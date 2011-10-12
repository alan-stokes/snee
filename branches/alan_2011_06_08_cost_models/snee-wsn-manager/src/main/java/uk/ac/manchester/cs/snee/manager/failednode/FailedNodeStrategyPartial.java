package uk.ac.manchester.cs.snee.manager.failednode;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.StrategyID;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.CandiateRouter;
import uk.ac.manchester.cs.snee.manager.failednode.metasteiner.MetaSteinerTreeException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @author stokesa6
 *class designed to encapsulate the partial framework of adapting just what needs to be adapted
 */

public class FailedNodeStrategyPartial extends FailedNodeStrategyAbstract
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 1161096834465452081L;
  @SuppressWarnings("unused")
  private boolean spacePinned;
  private boolean timePinned;
  private AgendaIOT oldAgenda;
  private Integer numberOfRoutingTreesToWorkOn = 0;
  private static int choice;
  private String sep = System.getProperty("file.separator");
  private File partialFolder; 
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public FailedNodeStrategyPartial(AutonomicManager autonomicManager, 
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
    this.qep.getIOT().setID("OldIOT");
    this.oldAgenda = this.qep.getAgendaIOT();
    this.numberOfRoutingTreesToWorkOn = numberOfTrees;
  }
  
  private void setUpFolders(AutonomicManager manager)
  {
    partialFolder = new File(outputFolder.toString() + sep + "partialStrategy");
    partialFolder.mkdir();
  }

  /**
   * if a route can't be calculated with the pinned sites, 
   * dynamically remove a site to allow adaptation.
   * @param oldIOT2
   * @param failedNodes
   * @param depinnedNodes
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws RouterException 
   */
  private void chooseNodeToDepin(IOT oldIOT, ArrayList<String> failedNodes,
                                      ArrayList<String> depinnedNodes) 
  throws 
  NumberFormatException, SNEEConfigurationException, 
  RouterException
  {
    Router router = new Router();
    //remove failed nodes out of new topology.
    Topology network = this.getWsnTopology();
    
    RT globalRT = router.doRouting(qep.getDAF().getPAF(), qep.getQueryName(), network, _metadata);
    ArrayList<Integer> globalSiteIDs = globalRT.getSiteIDs();
    ArrayList<Integer> qepSiteIDs = qep.getRT().getSiteIDs();
    
    Iterator<Integer> qepSiteIterator = qepSiteIDs.iterator();
    boolean found = false;
    Integer nextSite = null;
    while(qepSiteIterator.hasNext() && !found)
    {
      nextSite = qepSiteIterator.next();
      if(!globalSiteIDs.contains(nextSite) && 
         !depinnedNodes.contains(new Integer(nextSite).toString()) &&
         !failedNodes.contains(new Integer(nextSite).toString()))
        found = true;
    }
    if(!found)
    {
      boolean foundParent = false;
      while(!foundParent)
      {
        Iterator<String> failedNodeIDIterator = failedNodes.iterator();
        while(failedNodeIDIterator.hasNext() && !foundParent)
        {
          String failedNodeId = failedNodeIDIterator.next();
          Site failedSite = oldIOT.getRT().getSite(failedNodeId);
          Node parent = failedSite.getOutput(0);
          while(parent != null && !foundParent)
          {
            if(!depinnedNodes.contains(parent.getID()) &&
               !failedNodes.contains(parent.getID()) &&
               !globalRT.getRoot().getID().equals(parent.getID()))   
            {
              depinnedNodes.add(parent.getID());
              foundParent = true;
              found = true;
            }
            else
            {
              if(parent.getOutDegree() == 0)
                parent = null;
              else
                parent = parent.getOutput(0);
            } 
          }
        }
        if(!found) //if still not located a disconnected node
        {
          globalSiteIDs = qep.getIOT().getRT().getSiteIDs();
          failedNodeIDIterator = failedNodes.iterator();
          while(failedNodeIDIterator.hasNext())
          {
            globalSiteIDs.remove(new Integer(failedNodeIDIterator.next()));
          }
          Iterator<String> disconnectedNodeIDIterator = depinnedNodes.iterator();
          while(disconnectedNodeIDIterator.hasNext())
          {
            globalSiteIDs.remove(new Integer(disconnectedNodeIDIterator.next()));
          }
          globalSiteIDs.remove(new Integer(globalRT.getRoot().getID()));
          Random random = new Random();
          if(globalSiteIDs.size() == 0)
            throw new RouterException("No possible adapatation as all nodes disconnected and still no avilable routes");
          
          depinnedNodes.add(
              new Integer(globalSiteIDs.get(random.nextInt(globalSiteIDs.size()))).toString());
          found = true;
          foundParent = true;
        }
      }
    }
    else
    {
      depinnedNodes.add(nextSite.toString()); 
    }
  }

  /**
   * uses new routes to determine new QEP/adaptations
   * @param routeIterator @param failedNodes
   * @param depinnedNodes @param totalAdapatations
   * @throws SNEEException @throws SchemaMetadataException
   * @throws OptimizationException @throws SNEEConfigurationException
   * @throws MalformedURLException @throws WhenSchedulerException
   * @throws TypeMappingException @throws MetadataException
   * @throws UnsupportedAttributeTypeException @throws SourceMetadataException
   * @throws TopologyReaderException @throws SNEEDataSourceException
   * @throws CostParametersException @throws SNCBException
   * @throws SNEECompilerException 
   */
  private void generateQEPs(Iterator<RT> routeIterator, ArrayList<String> failedNodes, 
                                    ArrayList<String> depinnedNodes, 
                                    List<Adaptation> totalAdapatations)
  throws SNEEException, SchemaMetadataException, 
         OptimizationException, SNEEConfigurationException, 
         MalformedURLException, TypeMappingException, 
         MetadataException, UnsupportedAttributeTypeException, 
         SourceMetadataException, TopologyReaderException, 
         SNEEDataSourceException, CostParametersException, 
         SNCBException, SNEECompilerException
  {
    choice++;
    File choiceFolderMain = new File(partialFolder.toString() + sep + "choices"); 
    choiceFolderMain.mkdir();
    
    while(routeIterator.hasNext())
    {
      //set up current objects
      RT routingTree =  routeIterator.next();
      Adaptation currentAdapatation = new Adaptation(qep, StrategyID.FailedNodePartial, choice);
      
      File choiceFolder = new File(choiceFolderMain.toString() + sep + "choice" + choice);
      choiceFolder.mkdir();
      //create pinned paf
      PAF paf = pinPhysicalOperators(this.qep.getIOT(), failedNodes, depinnedNodes);
      //run fragment paf though where scheduler.
      InstanceWhereSchedular instanceWhere = 
        new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), choiceFolder.toString());
      IOT newIOT = instanceWhere.getIOT();
      //run new iot though when scheduler and locate changes
      this.oldAgenda.setID("old Agenda");
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //output new and old agendas
      new FailedNodeStrategyPartialUtils(this).outputAgendas(newAgenda, qep.getAgendaIOT(), this.qep.getIOT(), newIOT, choiceFolder);
      boolean success = assessQEPsAgendas(this.qep.getIOT(), newIOT, oldAgenda, newAgenda, 
                                          timePinned, currentAdapatation, failedNodes, routingTree);
      currentAdapatation.setFailedNodes(failedNodes);
      if(success)
        totalAdapatations.add(currentAdapatation);
      choice++;
    } 
  }

  /**
   * creates a new routeing tree for the where scheduler
   * @param agenda2
   * @param iot2
   * @param failedNodes
   * @param depinnedNodes 
   * @param paf 
   * @param outputFolder2 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws SchemaMetadataException 
   * @throws RouterException 
   * @throws MetaSteinerTreeException 
   */
  private ArrayList<RT> createNewRoutingTrees(ArrayList<String> failedNodes, 
      ArrayList<String> depinnedNodes, PAF paf, RT oldRoutingTree, File outputFolder,
      boolean previousDidntMeetQoSExpectations) 
  throws 
  NumberFormatException, SNEEConfigurationException, 
  SchemaMetadataException, RouterException
  {
    Topology network = this.getWsnTopology();
    CandiateRouter router = new CandiateRouter(network, outputFolder);
    return genereateRouteingTrees(oldRoutingTree, failedNodes, depinnedNodes, "", 
                            numberOfRoutingTreesToWorkOn, router, previousDidntMeetQoSExpectations);
  }

  /**
   * recursive method which repeats itself till it finds a set of failed 
   * and disconnected nodes which allow new routes to be generated
   * @param oldRoutingTree
   * @param failedNodes
   * @param depinnedNodes
   * @param queryName
   * @param numberOfRoutingTreesToWorkOn
   * @param router
   * @return
   * @throws SchemaMetadataException
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   * @throws RouterException
   */
  private ArrayList<RT> genereateRouteingTrees(RT oldRoutingTree, ArrayList<String> failedNodes, 
                                         ArrayList<String> depinnedNodes, String queryName, 
                                         Integer numberOfRoutingTreesToWorkOn, CandiateRouter router,
                                         boolean previousDidntMeetQoSExpectations) 
  throws
  SchemaMetadataException, NumberFormatException, 
  SNEEConfigurationException, RouterException
  {
    ArrayList<RT> routes = new ArrayList<RT>();
    try
    {
      if(!previousDidntMeetQoSExpectations)
        routes = router.generateCompleteRouteingTrees(oldRoutingTree, failedNodes, depinnedNodes, 
                                       queryName, numberOfRoutingTreesToWorkOn);
      if(routes.size() == 0)
      {
        chooseNodeToDepin(this.qep.getIOT(), failedNodes, depinnedNodes);
        System.out.println("No routes avilable, so disconnecting nodes" + depinnedNodes.toString());
        return genereateRouteingTrees(oldRoutingTree, failedNodes, depinnedNodes, queryName, 
                               numberOfRoutingTreesToWorkOn, router, false);
      }
      return routes;
    }
    catch(Exception e)
    {
     // e.printStackTrace();
      chooseNodeToDepin(this.qep.getIOT(), failedNodes, depinnedNodes);
      System.out.println("No routes avilable, so disconnecting nodes" + depinnedNodes.toString());
      return genereateRouteingTrees(oldRoutingTree, failedNodes, depinnedNodes, queryName, 
                             numberOfRoutingTreesToWorkOn, router, false);
    }
  }

  public AgendaIOT getOldAgenda()
  {
    return oldAgenda;
  }
  
  public IOT getOldIOT()
  {
    return this.qep.getIOT();
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
  
  /**
   * method used to recover from all types of failures (no routes, routes which break QoS 
   * expectations etc
   */
  private  List<Adaptation> adaptationAttempt(ArrayList<String> failedNodes,
                                              ArrayList<String> depinnedNodes, 
                                              PAF paf, RT rt, File partialFolder2, 
                                              List<Adaptation> totalAdapatations,
                                              boolean previouslyFailedToMeetQoSExpectations) 
  throws 
  NumberFormatException, SNEEConfigurationException, 
  SchemaMetadataException
  {
    ArrayList<RT> routingTrees;
    try
    {
      routingTrees = createNewRoutingTrees(failedNodes, depinnedNodes, paf, this.qep.getIOT().getRT(), 
                                           partialFolder, previouslyFailedToMeetQoSExpectations);
    }
    catch (RouterException e)
    {
      return totalAdapatations;
    }
    Iterator<RT> routeIterator = routingTrees.iterator();
    choice = 0;
    try
    {
      generateQEPs(routeIterator, failedNodes, depinnedNodes, totalAdapatations);
      return totalAdapatations;
    }
    catch(Exception e)
    {
      System.out.println("Routes generated didnt agree with QoS trying with larger scope");
      e.printStackTrace();
      return adaptationAttempt(failedNodes, depinnedNodes, paf, this.qep.getIOT().getRT(), 
                                         partialFolder, totalAdapatations, true);
    }
  }  
  
  /**
   * @throws RouterException 
   * MAIN METHOD TO ENTER PATIAL ADAPTATION
   * @throws  
   */
  @Override
  public List<Adaptation> adapt(ArrayList<String> failedNodes) 
  throws NumberFormatException, SNEEConfigurationException, SchemaMetadataException, 
  SNEECompilerException, MalformedURLException, SNEEException, OptimizationException,
  TypeMappingException, MetadataException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException 
  { 
    System.out.println("Running Failed Node FrameWork Partial"); 
    qep = (SensorNetworkQueryPlan) manager.getCurrentQEP();
    setUpFolders(manager);
    new FailedNodeStrategyPartialUtils(this).outputTopologyAsDotFile(partialFolder, 
                                                                     sep + "topology.dot");
    //setup collectors
    PAF paf = this.qep.getIOT().getPAF(); 
    ArrayList<String> disconnectedNodes = new ArrayList<String>();
    List<Adaptation> totalAdapatations = new ArrayList<Adaptation>();
    return adaptationAttempt(failedNodes, disconnectedNodes, paf, this.qep.getIOT().getRT(), partialFolder, 
                             totalAdapatations, false);
  }
}
