package uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

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
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.FailedNodeStrategyAbstract;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.FailedNodeLocalLogicalOverlayUtils;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayGenerator;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetworkUtils;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.ChoiceAssessorPreferenceEnum;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.RobustSensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.UnreliableChannelAgendaReduced;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.UnreliableChannelAgendaReducedUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

/**
 * 
 * @author alan
 *class which encapsulates the local framework using clusters and equivalence relations
 */
public class LogicalOverlayStrategy extends FailedNodeStrategyAbstract
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -7562607134737502147L;
  private Topology network = null;
  private LogicalOverlayNetwork logicalOverlay;
  private File localFolder;
  private String sep = System.getProperty("file.separator");
  private MetadataManager _metadataManager;
  /**
   * constructor
   * @param autonomicManager
   */
  public LogicalOverlayStrategy(AutonomicManagerImpl autonomicManager, 
                                 SourceMetadataAbstract _metadata,  MetadataManager _metadataManager)
  {
    super(autonomicManager, _metadata); 
    this._metadataManager = _metadataManager;
    setupFolders(outputFolder);
    cloner = new Cloner();
    cloner.dontClone(Logger.class);
  }
	
  /**
   * helper constructor for objects that do not require a predefined set of the energy sites. 
   */
  public void initilise(QueryExecutionPlan oldQep, Integer noTrees) 
  throws SchemaMetadataException, TypeMappingException, OptimizationException, 
  IOException, SNEEConfigurationException, CodeGenerationException 
  {
    initilise(oldQep, noTrees, null);
  }
  
  /**
   * sets up framework by detecting equivalent nodes and placing them in a cluster
   * @param oldQep
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   * @throws CodeGenerationException 
   * @throws ClassNotFoundException 
   */
  public void initilise(QueryExecutionPlan oldQep, Integer noTrees,
                        HashMap<String, RunTimeSite> copyOfRunningSites) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException, SNEEConfigurationException,
  CodeGenerationException
  {  
    this.currentQEP = (SensorNetworkQueryPlan) oldQep;
    logicalOverlay = new LogicalOverlayNetwork();
    network = getWsnTopology();
    String choice = SNEEProperties.getSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE);
    if(choice.equals(ChoiceAssessorPreferenceEnum.Local.toString()) || choice.equals(ChoiceAssessorPreferenceEnum.Best.toString()))
    {
      System.out.println("Determining Logical Overlay network");
      LogicalOverlayGenerator logcalOverlayGenerator = 
      new LogicalOverlayGenerator(network, this.currentQEP, this.manager, localFolder, _metadata, _metadataManager);
      logicalOverlay =  logcalOverlayGenerator.generateOverlay((SensorNetworkQueryPlan) oldQep, this);
      int k_resilence_level = SNEEProperties.getIntSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_LEVEL);
      //if k_resilence level is zero, and no clusters are found, then a empty overlay is satifisable.
      if(logicalOverlay == null && k_resilence_level == 0)
      {
        logicalOverlay = new LogicalOverlayNetwork();
        logicalOverlay.setQep(currentQEP);
      }
      //if no overlay is generated, then throw error
      if(logicalOverlay == null)
      {
        throw new SchemaMetadataException("current metatdata does not support a logical overlay structure " +
            "with the current k resilience value. Possible solutions is to reduce the k resilience level, or " +
            "add more nodes to compensate");
      }
      
      manager.setCurrentQEP(logicalOverlay.getQep());
      new FailedNodeLocalLogicalOverlayUtils(logicalOverlay, localFolder).outputAsTextFile();
      System.out.println("Finished Determining Logical Overlay network");
    }
  }
  
  /**
   * sets up framework given a logical overlay network
   * @param oldQep
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   * @throws CodeGenerationException 
   * @throws ClassNotFoundException 
   */
  public void initilise(RobustSensorNetworkQueryPlan rQEP, int noTrees,
      LogicalOverlayNetwork logicalOverlayNetwork)
  {
    this.currentQEP = (SensorNetworkQueryPlan) rQEP;
    logicalOverlay = logicalOverlayNetwork;
    network = getWsnTopology(); 
  }
  
  /**
   * sets up framework given a logical overlay network
   * @param oldQep
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   * @throws CodeGenerationException 
   * @throws ClassNotFoundException 
   */
  public void initilise(SensorNetworkQueryPlan rQEP, int noTrees,
      LogicalOverlayNetwork logicalOverlayNetwork)
  {
    this.currentQEP = rQEP;
    logicalOverlay = logicalOverlayNetwork;
    network = getWsnTopology(); 
  }

  private void setupFolders(File outputFolder)
  {
    localFolder = new File(outputFolder.toString() + sep + "localStrategy");
    localFolder.mkdir();
  }
  
  /**
   * used to update stragies where to output data files
   * @param outputFolder
   */
  public void updateFrameWorkStorage(File outputFolder)
  {
    this.outputFolder = outputFolder;
    setupFolders(outputFolder);
  }
  
  /**
   * checks if a cluster exists for a specific node
   * @param primary (cluster head)
   * @return true if cluster exists, false otherwise
   */
  public boolean isThereACluster(String primary)
  {
    if(logicalOverlay.getEquivilentNodes(primary).size() != 0)
      return true;
    else
      return false;
  }
  
  /**
   * checks if a cluster exists, if so then gives the first node in the cluster, otherwise 
   * gives null pointer.
   * @param primary cluster head
   * @return new cluster head or null 
   */
  public String retrieveNewClusterHead(String primary, List<Node> list, Node parent)
  {
    return retrieveNewClusterHead(primary, list, parent, this.logicalOverlay);
  }
  
  /**
   * checks if a cluster exists, if so then gives the first node in the cluster, otherwise 
   * gives null pointer.
   * @param primary cluster head
   * @return new cluster head or null 
   */
  public String retrieveNewClusterHead(String primary, List<Node> list, Node parent, LogicalOverlayNetwork overlay)
  {
    if(isThereACluster(primary, overlay))
    {
      return overlay.getReplacement(primary, list, parent, network);
    }
    else
      return null;
  }
  
  /**
   * checks if a cluster exists, if so then gives the first node in the cluster, otherwise 
   * gives null pointer.
   * @param primary cluster head
   * @return new cluster head or null 
   */
  public String retrieveNewClusterHead(String primary, List<Node> list, Node parent, 
                                       LogicalOverlayNetworkHierarchy overlay)
  {
    if(isThereACluster(primary, overlay))
    {
      return overlay.getReplacement(primary, list, parent, network);
    }
    else
      return null;
  }
  
  

  @Override
  public boolean canAdapt(String failedNode)
  {
    return canAdapt(failedNode, this.logicalOverlay);
  }
  
  /**
   * checks if for this overlay, an adaptation can be done
   */
  public boolean canAdapt(String failedNode, LogicalOverlayNetwork overlay)
  {
    if(failedNode == null)
      return false;
    else
      return isThereACluster(failedNode, overlay);
  }
  
  private boolean isThereACluster(String primary,
      LogicalOverlayNetwork overlay)
  {
    if(overlay.getEquivilentNodes(primary).size() != 0)
      return true;
    else
      return false;
  }

  @Override
  public boolean canAdaptToAll(ArrayList<String> failedNodes)
  {
    return canAdaptToAll(failedNodes, this.logicalOverlay);
  }
  
  /**
   * checks if for a specific overlay, if it can adapt to all the failed nodes  
   * @param failedNodeIDs
   * @param overlay
   * @return
   */
  private boolean canAdaptToAll(ArrayList<String> failedNodes,
                                LogicalOverlayNetwork overlay)
  {
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    boolean success = true;
    while(failedNodeIterator.hasNext() && success)
    {
      if(!canAdapt(failedNodeIterator.next(), overlay))
        success = false;
    }
    return success;
  }
  

  private void rewireRoutingTree(String failedNodeID, String equivilentNodeID, RT currentRoutingTree) 
  throws 
  OptimizationException
  {
    Node equivilentNode = network.getNode(equivilentNodeID);
    equivilentNode.clearInputs();
    equivilentNode.clearOutputs();
    Node failedNode = currentRoutingTree.getSiteTree().getNode(failedNodeID);
    currentRoutingTree.getSiteTree().removeNodeWithoutLinkage(failedNode);
    currentRoutingTree.getSiteTree().addNode(equivilentNode);
    //sort out outputs
    Node output = failedNode.getOutput(0);
    output.removeInput(failedNodeID);
    output.addInput(equivilentNode);
    equivilentNode.addOutput(output);
    
    //sort out inputs
    Iterator<Node> inputIterator = failedNode.getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      Node input = inputIterator.next();
      input.clearOutputs();
      input.addOutput(equivilentNode);
      equivilentNode.addInput(input);
    }
    currentRoutingTree.getSiteTree().updateNodesAndEdgesColls(currentRoutingTree.getSiteTree().getRoot());
  }

  private void rewireOperators(IOT clonedIOT, String failedNodeID, String equivilentNodeID, IOT iot, Site failedSite)
  throws OptimizationException
  {
    ///children first
    Site equivilentSite = clonedIOT.getRT().getSite(equivilentNodeID);
    Iterator<Node> chidlrenIterator = failedSite.getInputsList().iterator();
    while(chidlrenIterator.hasNext())
    {
      Site child = clonedIOT.getRT().getSite(chidlrenIterator.next().getID());
      Iterator<InstanceExchangePart> exchangeIterator = child.getInstanceExchangeComponents().iterator();
      while(exchangeIterator.hasNext())
      {
        InstanceExchangePart part = exchangeIterator.next();
        if(part.getNext() != null && part.getNext().getSite().getID().equals(failedNodeID))
        {
          InstanceExchangePart nextPart = part.getNext();
          Iterator<InstanceExchangePart> eqivExchangeIterator = 
             equivilentSite.getInstanceExchangeComponents().iterator();
          while(eqivExchangeIterator.hasNext())
          {
            InstanceExchangePart eqPart = eqivExchangeIterator.next();
            if(nextPart.getPreviousId().equals(eqPart.getPreviousId()))
            {
              clonedIOT.removeAllEdgesWithDefinedPosition(part, true);
              assert eqPart.getSite().getID().equals(equivilentSite.getID());
              assert !part.getSite().getID().equals(equivilentSite.getID());
              clonedIOT.addEdge(part, eqPart);
              part.setNextExchange(eqPart);
            }
          }
        }
      }
    }
      
    //parent 
    Site outputSite = clonedIOT.getRT().getSite(failedSite.getOutput(0).getID());
    Iterator<InstanceExchangePart> exchangeIterator = outputSite.getInstanceExchangeComponents().iterator();
    while(exchangeIterator.hasNext())
    {
      InstanceExchangePart parentpart = exchangeIterator.next();
      if(parentpart.getPrevious() != null && parentpart.getPrevious().getSite().getID().equals(failedNodeID))
      {
        InstanceExchangePart previousPart = parentpart.getPrevious();
        Iterator<InstanceExchangePart> eqivExchangeIterator = 
           equivilentSite.getInstanceExchangeComponents().iterator();
        while(eqivExchangeIterator.hasNext())
        {
          InstanceExchangePart eqPart = eqivExchangeIterator.next();
          if(previousPart.getID().equals(eqPart.getPreviousId()))
          {
            clonedIOT.removeAllEdgesWithDefinedPosition(parentpart, false);
            clonedIOT.addEdge(eqPart, parentpart);
            parentpart.setPreviousExchange(eqPart);
          }
        }
      }
    }
    //remove dangling pointers to failed node stuff
    failedSite.clearInstanceExchangeComponents();
    failedSite.clearExchangeComponents();
    clonedIOT.removeSiteFromMapping(failedSite);
    clonedIOT.removeFragment(failedSite);
    
    Iterator<InstanceOperator> opInstIter = clonedIOT.treeIterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) 
    {
      InstanceOperator opInst = opInstIter.next();
      if(opInst.getSite().getID().equals(failedSite.getID()))
      {
        opInst.getOutput(0).removeInput(opInst.getID());
        clonedIOT.getOperatorTree().removeNode(opInst.getID());
        if(opInst.getOutput(0) instanceof InstanceExchangePart)
        {
          InstanceExchangePart exOpInst = (InstanceExchangePart) opInst.getOutput(0);
          exOpInst.removeInput(opInst);
          exOpInst.setPrev(null);
        }
      }
    }
  }
  
  
  @Override
  public List<Adaptation> adapt(ArrayList<String> failedNodeIDs) 
  throws OptimizationException
  {
    network = getWsnTopology();
    return adapt(failedNodeIDs, this.logicalOverlay);
  }
  
  /**
   * adapts to node failures on a specific overlay (used in calculating the 
   */
  public List<Adaptation> adapt(ArrayList<String> failedNodeIDs, LogicalOverlayNetwork overlay) 
  throws OptimizationException
  {
    try
    {
      System.out.println("Running Failed Node FrameWork Local");
      List<Adaptation> adapatation = new ArrayList<Adaptation>();
      if(this.canAdaptToAll(failedNodeIDs, overlay))
      {
        if(overlay instanceof LogicalOverlayNetworkHierarchy)
        {
          return executeHierarchyAdaptation(failedNodeIDs, (LogicalOverlayNetworkHierarchy) overlay);
        }
        else
        {
          return executeAdaptation(failedNodeIDs, overlay);
        }
      }
      else
        return adapatation;
    }
    catch(Exception e)
    {
      System.out.println("local failed");
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(0);
      return null; 
    }
  }
  
  private List<Adaptation> executeAdaptation(ArrayList<String> failedNodeIDs,
                                             LogicalOverlayNetwork overlay) 
  throws FileNotFoundException, IOException, OptimizationException, SchemaMetadataException
  {
    List<Adaptation> adapatation = new ArrayList<Adaptation>();
    Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
    Adaptation adapt = new Adaptation(overlay.getQep(), StrategyIDEnum.FailedNodeLocal, 1);
  
    new IOTUtils().storeIOT(overlay.getQep().getIOT(), localFolder);
    String oldid = overlay.getQep().getIOT().getID();
    IOT clonedIOT = overlay.getQep().getIOT();
    RT currentRoutingTree = clonedIOT.getRT();
    while(failedNodeIDsIterator.hasNext())
    {
      String failedNodeID = failedNodeIDsIterator.next();
      Site failedSite = cloner.deepClone(overlay.getQep().getRT().getSite(failedNodeID));
      String equivilentNodeID = 
        retrieveNewClusterHead(failedNodeID,
                               currentRoutingTree.getSite(failedNodeID).getInputsList(),
                               currentRoutingTree.getSite(failedNodeID).getOutput(0), overlay);
      if(failedNodeID == null)
        return null;
      //sort out adaptation data structs.
      adapt.addActivatedSite(equivilentNodeID);
      Iterator<Node> redirectedNodesIterator = overlay.getQep().getRT().getSite(failedNodeID).getInputsList().iterator();
      while(redirectedNodesIterator.hasNext())
      {
        adapt.addRedirectedSite(redirectedNodesIterator.next().getID());
      }
      //rewire routing tree
      rewireRoutingTree(failedNodeID, equivilentNodeID, currentRoutingTree);
      new RTUtils(currentRoutingTree).exportAsDOTFile(localFolder.toString() + sep + "new RT");
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay Before nodes ");
      //rewire children
      rewireOperators(clonedIOT, failedNodeID, equivilentNodeID, overlay.getQep().getIOT(), failedSite);
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay after nodes");
    }
    new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iot", "iot with eqiv nodes", true);
    
    new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotInputs", "iot with eqiv nodes", true, true);
    
    new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay");
    try
    {
      IOT newIOT = clonedIOT;
      newIOT.setID("new iot");
      IOTUtils utils = new IOTUtils(newIOT, currentQEP.getCostParameters());
      utils.disconnectExchanges();
     // newIOT.setDAF(utils.convertToDAF());
      utils.reconnectExchanges();
      new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotAfterReconnect", "iot with eqiv nodes", true);
      //new DAFUtils(newIOT.getDAF()).exportAsDotFile(localFolder.toString() + sep + "daf");
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay after disconnect and reconnect");
      
      
      if(currentQEP instanceof RobustSensorNetworkQueryPlan)
      {
        RobustSensorNetworkQueryPlan rQEP = (RobustSensorNetworkQueryPlan) currentQEP;
        RobustSensorNetworkQueryPlan newRQEP = 
          new RobustSensorNetworkQueryPlan(currentQEP, rQEP.getUnreliableAgenda().getActiveLogicalOverlay(), 
                                           rQEP.getUnreliableAgenda());
        adapt.setNewQep(newRQEP);
        adapt.setFailedNodes(failedNodeIDs);
        adapatation.add(adapt);
        return adapatation;
      }
      else
      {
        //run new iot though when scheduler and locate changes
        AgendaIOT  newAgendaIOT = doSNWhenScheduling(newIOT, currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
       // Agenda newAgenda = doOldSNWhenScheduling(newIOT.getDAF(), currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
        //output new and old agendas
        new LogicalOverlayStrategyUtils().outputAgendas(newAgendaIOT, currentQEP.getAgendaIOT(), 
                                                             currentQEP.getIOT(), newIOT, localFolder);
        IOT oldIOT = new IOTUtils().retrieveIOT(localFolder, oldid);
        boolean success = assessQEPsAgendas(oldIOT, newIOT, overlay.getQep().getAgendaIOT(), newAgendaIOT, null, 
                                          false, adapt, failedNodeIDs, currentRoutingTree, false,
                                          this.currentQEP.getDLAF(), this.currentQEP.getID(), this.currentQEP.getCostParameters());
        adapt.setFailedNodes(failedNodeIDs);
        if(success)
          adapatation.add(adapt);
        return adapatation;
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }
    return adapatation; 
  }

  private List<Adaptation> executeHierarchyAdaptation(ArrayList<String> failedNodeIDs,
                                                      LogicalOverlayNetworkHierarchy overlay)
  throws FileNotFoundException, IOException, OptimizationException, SchemaMetadataException
  {
    List<Adaptation> adapatation = new ArrayList<Adaptation>();
    Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
    Adaptation adapt = new Adaptation(overlay.getQep(), StrategyIDEnum.FailedNodeLocal, 1);
  
    new IOTUtils().storeIOT(overlay.getQep().getIOT(), localFolder);
    String oldid = overlay.getQep().getIOT().getID();
    IOT clonedIOT = overlay.getQep().getIOT();
    RT currentRoutingTree = clonedIOT.getRT();
    while(failedNodeIDsIterator.hasNext())
    {
      String failedNodeID = failedNodeIDsIterator.next();
      Site failedSite = cloner.deepClone(overlay.getQep().getRT().getSite(failedNodeID));
      String equivilentNodeID = 
        retrieveNewClusterHead(failedNodeID,
                               currentRoutingTree.getSite(failedNodeID).getInputsList(),
                               currentRoutingTree.getSite(failedNodeID).getOutput(0), overlay);
      //sort out adaptation data structs.
      adapt.addActivatedSite(equivilentNodeID);
      Iterator<Node> redirectedNodesIterator = overlay.getQep().getRT().getSite(failedNodeID).getInputsList().iterator();
      while(redirectedNodesIterator.hasNext())
      {
        adapt.addRedirectedSite(redirectedNodesIterator.next().getID());
      }
      //rewire routing tree
      rewireRoutingTree(failedNodeID, equivilentNodeID, currentRoutingTree);
      new RTUtils(currentRoutingTree).exportAsDOTFile(localFolder.toString() + sep + "new RT");
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay Before nodes ");
      //rewire children
      rewireOperators(clonedIOT, failedNodeID, equivilentNodeID, overlay.getQep().getIOT(), failedSite);
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay after nodes");
      overlay.updateClusters(failedNodeID, equivilentNodeID);
      cleanNodes(clonedIOT, failedNodeID);
    }
    new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iot", "iot with eqiv nodes", true);
    
    new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotInputs", "iot with eqiv nodes", true, true);
    
    new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay");
    try
    {
      IOT newIOT = clonedIOT;
      newIOT.setID("new iot");
      IOTUtils utils = new IOTUtils(newIOT, currentQEP.getCostParameters());
      utils.disconnectExchanges();
     // newIOT.setDAF(utils.convertToDAF());
      utils.reconnectExchanges();
      new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotAfterReconnect", "iot with eqiv nodes", true);
      //new DAFUtils(newIOT.getDAF()).exportAsDotFile(localFolder.toString() + sep + "daf");
      new LogicalOverlayNetworkUtils().exportAsADotFile(clonedIOT, overlay, localFolder.toString() + sep + "iot with overlay after disconnect and reconnect");
      
      
      if(currentQEP instanceof RobustSensorNetworkQueryPlan)
      {
        RobustSensorNetworkQueryPlan rQEP = (RobustSensorNetworkQueryPlan) currentQEP;
        boolean allowDiscontinuousSensing = 
          SNEEProperties.getBoolSetting(SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
        UnreliableChannelAgendaReduced newAgenda = 
          new UnreliableChannelAgendaReduced(overlay, currentQEP, network, allowDiscontinuousSensing);
        new UnreliableChannelAgendaReducedUtils(newAgenda, clonedIOT, false, new ArrayList<String>())
        .generateImage(outputFolder.toString(), "finished Agenda");
        RobustSensorNetworkQueryPlan newRQEP = 
          new RobustSensorNetworkQueryPlan(currentQEP, newAgenda.getActiveLogicalOverlay(), 
                                           newAgenda);
        adapt.setNewQep(newRQEP);
        adapt.setFailedNodes(failedNodeIDs);
        adapatation.add(adapt);
        return adapatation;
      }
      else
      {
        //run new iot though when scheduler and locate changes
        AgendaIOT  newAgendaIOT = doSNWhenScheduling(newIOT, currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
       // Agenda newAgenda = doOldSNWhenScheduling(newIOT.getDAF(), currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
        //output new and old agendas
        new LogicalOverlayStrategyUtils().outputAgendas(newAgendaIOT, currentQEP.getAgendaIOT(), 
                                                        currentQEP.getIOT(), newIOT, localFolder);
        IOT oldIOT = new IOTUtils().retrieveIOT(localFolder, oldid);
        boolean success = assessQEPsAgendas(oldIOT, newIOT, overlay.getQep().getAgendaIOT(), newAgendaIOT, null, 
                                          false, adapt, failedNodeIDs, currentRoutingTree, false,
                                          this.currentQEP.getDLAF(), this.currentQEP.getID(), this.currentQEP.getCostParameters());
        adapt.setFailedNodes(failedNodeIDs);
        if(success)
          adapatation.add(adapt);
        return adapatation;
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }
    return adapatation;    
  }

  
  //helper method which removes failednode from other nodes outputs (due to unreliablechannel connecting)
  private void cleanNodes(IOT clonedIOT, String failedNodeID)
  {
    Iterator<Site> siteIterator = clonedIOT.getAllSites().iterator();
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      ArrayList<Node> outputs = new ArrayList<Node>();
      outputs.addAll(site.getOutputsList());
      int index = indexOfSameNodeID(outputs, failedNodeID);
      if(index != -1)
        site.removeOutput(outputs.get(index));
    }
    
  }

  private int indexOfSameNodeID(ArrayList<Node> outputs, String failedNodeID)
  {
    int counter = 0;
    Iterator<Node> outputIterator = outputs.iterator();
    while(outputIterator.hasNext())
    {
      Node output = outputIterator.next();
      if(output.getID().equals(failedNodeID))
        return counter;
      else
        counter++;
    }
    return -1;
  }

  @Override
  public void update(Adaptation finalChoice)
  {
    update(finalChoice, this.logicalOverlay);
  }
  

  public void update(Adaptation finalChoice, LogicalOverlayNetwork network)
  {
    Iterator<String> failedNodeIterator = finalChoice.getFailedNodes().iterator();
    while(failedNodeIterator.hasNext())
    {
      String failedNode = failedNodeIterator.next();
      Iterator<String> activatedSitesIterator = finalChoice.getActivateSites().iterator();
      while(activatedSitesIterator.hasNext())
      {
        String activedSite = activatedSitesIterator.next();
        if(network.getEquivilentNodes(failedNode).contains(activedSite))
        {
          network.updateCluster(failedNode, activedSite);
          //look for a extent which had the failed node as its contributer.
          //if so, remove from the meta-data and replace with the activated node
          List<String> extentNames = this._metadata.getExtentNames();
          Iterator<String> extentIterator = extentNames.iterator();
          ArrayList<String> extentsInvolved = new ArrayList<String>();
          while(extentIterator.hasNext())
          {
            String extent = extentIterator.next();
            SensorNetworkSourceMetadata source = (SensorNetworkSourceMetadata) _metadata;
            ArrayList<Integer> sourceSites = source.getSourceSites(extent);
            if(sourceSites.contains(new Integer(failedNode)))
            {
              extentsInvolved.add(extent);
            }
          }
          SensorNetworkSourceMetadata source = (SensorNetworkSourceMetadata) _metadata;
          source.removeSourceSite(new Integer(failedNode));
          source.addSourceSite(new Integer(activedSite), extentsInvolved);
        }
      }      
    }
    Iterator<String> reproNodeIterator = finalChoice.getReprogrammingSites().iterator();
    while(reproNodeIterator.hasNext())
    {
      String reprogrammedNode = reproNodeIterator.next();
      network.removeNode(reprogrammedNode);
    }
    this.logicalOverlay.setQep(finalChoice.getNewQep());
  }

  public LogicalOverlayNetwork getLogicalOverlay()
  {
    return this.logicalOverlay;
  }
  
  
  public void setQEP(SensorNetworkQueryPlan  currentQEP)
  {
    if(this.logicalOverlay != null)
      this.logicalOverlay.setQep(currentQEP);
  }

}
