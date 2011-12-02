package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayGenerator;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetwork; 
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalLogicalOverlayUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

/**
 * 
 * @author alan
 *class which encapsulates the local framework using clusters and equivalence relations
 */
public class FailedNodeStrategyLocal extends FailedNodeStrategyAbstract
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
  public FailedNodeStrategyLocal(AutonomicManagerImpl autonomicManager, 
                                 SourceMetadataAbstract _metadata,  MetadataManager _metadataManager)
  {
    super(autonomicManager, _metadata); 
    this._metadataManager = _metadataManager;
    setupFolders(outputFolder);
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
   */
  public void initilise(QueryExecutionPlan oldQep, Integer noTrees) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException, SNEEConfigurationException,
  CodeGenerationException
  {  
    
    this.currentQEP = (SensorNetworkQueryPlan) oldQep;
    logicalOverlay = new LogicalOverlayNetwork();
    network = getWsnTopology();
    LogicalOverlayGenerator logcalOverlayGenerator = 
      new LogicalOverlayGenerator(network, this.currentQEP, this.manager, localFolder, _metadata, _metadataManager);
    logicalOverlay =  logcalOverlayGenerator.generateOverlay((SensorNetworkQueryPlan) oldQep, this);
    if(logicalOverlay == null)
      throw new SchemaMetadataException("current metatdata does not support a logical overlay structure " +
          "with the current k resilience value. Possible solutions is to reduce the k resilience level, or " +
          "add more nodes to compensate");
    manager.setCurrentQEP(logicalOverlay.getQep());
    new FailedNodeLocalLogicalOverlayUtils(logicalOverlay, localFolder).outputAsTextFile();
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
    if(isThereACluster(primary))
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
    return isThereACluster(failedNode);
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

  private void rewireNodes(IOT clonedIOT, String failedNodeID, String equivilentNodeID)
  {
    ///children first
    Site failedSite = currentQEP.getRT().getSite(failedNodeID);
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
            if(nextPart.getID().equals(eqPart.getID()))
            {
              part.clearOutputs();
              part.addOutput(eqPart);
              part.setNextExchange(eqPart);
              eqPart.clearInputs();
              eqPart.addInput(part);
              part.setDestinitionSite(equivilentSite);
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
      InstanceExchangePart part = exchangeIterator.next();
      if(part.getPrevious() != null && part.getPrevious().getSite().getID().equals(failedNodeID))
      {
        InstanceExchangePart previousPart = part.getPrevious();
        Iterator<InstanceExchangePart> eqivExchangeIterator = 
           equivilentSite.getInstanceExchangeComponents().iterator();
        while(eqivExchangeIterator.hasNext())
        {
          InstanceExchangePart eqPart = eqivExchangeIterator.next();
          if(previousPart.getID().equals(eqPart.getID()))
          {
            part.replaceInput(previousPart, eqPart);
            part.setPreviousExchange(eqPart);
            eqPart.clearOutputs();
            eqPart.addOutput(part);
            part.setSourceSite(equivilentSite);
          }
        }
      }
    }
  }
  
  
  @Override
  public List<Adaptation> adapt(ArrayList<String> failedNodeIDs) 
  throws OptimizationException
  {
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
        Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
        Adaptation adapt = new Adaptation(overlay.getQep(), StrategyIDEnum.FailedNodeLocal, 1);
      
        IOT clonedIOT = cloner.deepClone(overlay.getQep().getIOT());
        RT currentRoutingTree = clonedIOT.getRT();
        while(failedNodeIDsIterator.hasNext())
        {
          String failedNodeID = failedNodeIDsIterator.next();
          String equivilentNodeID = 
            retrieveNewClusterHead(failedNodeID,
                                   currentRoutingTree.getSite(failedNodeID).getInputsList(),
                                   currentRoutingTree.getSite(failedNodeID).getOutput(0));
          //sort out adaptation data structs.
          adapt.addActivatedSite(equivilentNodeID);
          Iterator<Node> redirectedNodesIterator = overlay.getQep().getRT().getSite(failedNodeID).getInputsList().iterator();
          while(redirectedNodesIterator.hasNext())
          {
            adapt.addRedirectedSite(redirectedNodesIterator.next().getID());
          }
          //rewire routing tree
          rewireRoutingTree(failedNodeID, equivilentNodeID, currentRoutingTree);
          //rewire children
          rewireNodes(clonedIOT, failedNodeID, equivilentNodeID);
        }
        new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iot", "iot with eqiv nodes", true);
        
        new IOTUtils(clonedIOT, overlay.getQep().getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotInputs", "iot with eqiv nodes", true, true);
        try
        {
          IOT newIOT = clonedIOT;
          newIOT.setID("new iot");
          newIOT.setDAF(new IOTUtils(newIOT, currentQEP.getCostParameters()).convertToDAF());
        
          //run new iot though when scheduler and locate changes
          AgendaIOT newAgendaIOT = doSNWhenScheduling(newIOT, currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
          Agenda newAgenda = doOldSNWhenScheduling(newIOT.getDAF(), currentQEP.getQos(), currentQEP.getID(), currentQEP.getCostParameters());
          //output new and old agendas
          new FailedNodeStrategyLocalUtils().outputAgendas(newAgendaIOT, currentQEP.getAgendaIOT(), 
                                                               currentQEP.getIOT(), newIOT, localFolder);
        
          boolean success = assessQEPsAgendas(overlay.getQep().getIOT(), newIOT, overlay.getQep().getAgendaIOT(), newAgendaIOT, newAgenda, 
                                            false, adapt, failedNodeIDs, currentRoutingTree, false);
        
          adapt.setFailedNodes(failedNodeIDs);
          if(success)
            adapatation.add(adapt);
          return adapatation;
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        return adapatation;
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
        }
      }      
    }
    Iterator<String> reproNodeIterator = finalChoice.getReprogrammingSites().iterator();
    while(reproNodeIterator.hasNext())
    {
      String reprogrammedNode = reproNodeIterator.next();
      network.removeNode(reprogrammedNode);
    }
 
  }
  
  
  
  
  
}
