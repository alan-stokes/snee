package uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.planner.Planner;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.Model;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class LogicalOverlayGenerator
{
  private Topology network = null;
  private SensorNetworkQueryPlan currentQEP;
  private RT routingTree = null;
  private Cloner cloner = null;
  private int k_resilence_level = 0;
  private boolean k_resilence_sense = false;
  private AutonomicManagerImpl manager;
  private File localFolder = null;
  private String sep = System.getProperty("file.separator"); 
  private SourceMetadataAbstract _metadata = null;
  private MetadataManager _metadataManager = null;
  
  public LogicalOverlayGenerator(Topology network, SensorNetworkQueryPlan qep, 
                                 AutonomicManagerImpl manager, File localFolder, 
                                 SourceMetadataAbstract _metadata, MetadataManager _metadataManager)
  throws SNEEConfigurationException
  {
    this.network = network;
    this.currentQEP = qep;
    this.routingTree = qep.getRT();
    this.manager = manager;
    this.localFolder = localFolder;
    this._metadata = _metadata;
    this._metadataManager = _metadataManager;
    cloner = new Cloner();
    cloner.dontClone(Logger.class);
    k_resilence_level = SNEEProperties.getIntSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_LEVEL);
    k_resilence_sense = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_SENSE);
  }
  
  public LogicalOverlayNetwork generateOverlay(SensorNetworkQueryPlan qep, 
                                               LogicalOverlayStrategy failedNodeStrategyLocal) 
  throws SchemaMetadataException, TypeMappingException, OptimizationException, 
  IOException, CodeGenerationException, SNEEConfigurationException
  {
    new LogicalOverlayGeneratorUtils().storeQEP(qep, localFolder);
    LogicalOverlayNetwork superLogicalOverlay = setUpPhase();
    
    //outputs
    new LogicalOverlayNetworkUtils().storeOverlayAsFile(superLogicalOverlay, new File(localFolder.toString() + sep + "superOverlay"));
    new LogicalOverlayNetworkUtils().storeOverlayAsTextFile(superLogicalOverlay, new File(localFolder.toString() + sep + "superOverlay.txt"));
   
    
    ArrayList<LogicalOverlayNetwork> setsOfLogicalOverlays = reductionPhase(superLogicalOverlay);
  //  setsOfLogicalOverlays = KResilenceCheck(setsOfLogicalOverlays);
    LogicalOverlayNetwork logicalOverlay = 
      assessmentPhase(setsOfLogicalOverlays, failedNodeStrategyLocal, qep.getID(), qep.getRT());
    if(logicalOverlay != null)
      new LogicalOverlayGeneratorUtils().storeOverlayAsText(logicalOverlay, localFolder);
    return logicalOverlay;
  }

  private void transferQEPsToCandiates(LogicalOverlayNetwork currentOverlay,
                                       String qepID) 
  throws IOException
  {
    SensorNetworkQueryPlan qep = new  LogicalOverlayGeneratorUtils().retrieveQEP(localFolder, qepID);
    currentOverlay.setQep(qep);
    PhysicalToLogicalConversion transfer = 
      new PhysicalToLogicalConversion(currentOverlay, network, localFolder);
    transfer.transferQEPs();
    new LogicalOverlayNetworkUtils().exportAsADotFile(qep.getIOT(), currentOverlay, localFolder + sep + currentOverlay.getId());
    
  }

  /**
   * takes the set of sets of cluster and evaluates them based off energy model to
   *  determine the best suited cluster for extending the query lifetime 
   * @param setsOfLogicalOverlays 
   * @param failedNodeStrategyLocal 
   * @param qep 
   * @throws CodeGenerationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   * @throws ClassNotFoundException 
   */
  private LogicalOverlayNetwork assessmentPhase(ArrayList<LogicalOverlayNetwork> setsOfLogicalOverlays,
                                                LogicalOverlayStrategy failedNodeStrategyLocal,
                                                final String qepID, RT qepRT) 
  throws IOException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, CodeGenerationException, 
  SNEEConfigurationException 
  {
    int overlayIndex = 0;
    String bestOverlayNetwork = null;
    Double bestMinLifetime = Double.MIN_VALUE;
    while(overlayIndex < setsOfLogicalOverlays.size())
    {
      LogicalOverlayNetwork currentOverlay = setsOfLogicalOverlays.get(overlayIndex);
      if(hasCorrectLevelsOfResileince(currentOverlay, qepRT))
      {
        transferQEPsToCandiates(currentOverlay, qepID);
        Double minLifetime = determineMinumalLifetime(currentOverlay, failedNodeStrategyLocal);
        if(minLifetime >= bestMinLifetime)
        {
          bestOverlayNetwork = currentOverlay.getId();
          bestMinLifetime = minLifetime;
        }
        //remove copies of the overlays, to free up memory.
        currentOverlay = null;
        setsOfLogicalOverlays.set(overlayIndex, null);
        System.gc();
      }
      overlayIndex++;
    }
    //allow the model to compile the next thing.
    Model.setCompiledAlready(false);
    //reads in the best overlay from file (stored in a file for memory reasons
    if(bestOverlayNetwork != null)
    {
      LogicalOverlayNetworkUtils utils = new LogicalOverlayNetworkUtils();
      return  utils.retrieveOverlayFromFile(new File(localFolder + sep + "OTASection"), bestOverlayNetwork);
    }
    else
      return null;
  }

  /**
   * takes an overlay and determines the minimal lifetime from all clusters.
   * @param currentOverlay
   * @param failedNodeStrategyLocal 
   * @return
   * @throws CodeGenerationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   */
  private Double determineMinumalLifetime(LogicalOverlayNetwork currentOverlay,
                                          LogicalOverlayStrategy failedNodeStrategyLocal) 
  throws IOException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, CodeGenerationException, SNEEConfigurationException
  {
    HashMap<String, RunTimeSite> runningSites = manager.getCopyOfRunningSites();
    //remove OTA effects from running sites
    SensorNetworkQueryPlan sqep = (SensorNetworkQueryPlan)currentQEP;
    Adaptation overlayOTAProgramCost = new Adaptation(sqep, StrategyIDEnum.Orginal, 0);
    Iterator<Integer> siteIdIterator = sqep.getRT().getSiteIDs().iterator();
    while(siteIdIterator.hasNext())
    {
      Integer siteIDInt = siteIdIterator.next();
      overlayOTAProgramCost.addReprogrammedSite(siteIDInt.toString());
    }
    overlayOTAProgramCost.setNewQep(sqep);
    File outputFolder = new File(localFolder + sep + "OTASection");
    outputFolder.mkdir();
    Planner planner = new Planner(manager, _metadata, _metadataManager, runningSites, outputFolder);
    planner.assessOverlayCosts(outputFolder, overlayOTAProgramCost, currentOverlay, failedNodeStrategyLocal);
    return overlayOTAProgramCost.getLifetimeEstimate();
  }

  /**
   * checks the level of resilience in each cluster, returns false if any one of them meets specified levels
   * @param current
   * @return
   */
  private boolean hasCorrectLevelsOfResileince(LogicalOverlayNetwork current, RT rt)
  {
    Iterator<Integer> keys = rt.getSiteIDs().iterator();
    while(keys.hasNext())
    {
      Integer key = keys.next();
      ArrayList<String> cluster = current.getEquivilentNodes(key.toString());
      if(!routingTree.getRoot().getID().equals(key.toString()))  
        if((routingTree.getSite(key).isSource() && k_resilence_sense && cluster.size() < k_resilence_level) 
           || (!routingTree.getSite(key).isSource() && cluster.size() < k_resilence_level))
        {
          return false; 
        }
    }
    return true;
  }

  /**
   * takes the setup cluster (not correct cluster as candidates may not be able to talk to 
   * each other. generates sets of clusters which are all valid cluster formats
   * @param superLogicalOverlay 
   * @throws IOException 
   */
  private ArrayList<LogicalOverlayNetwork> reductionPhase(LogicalOverlayNetwork superLogicalOverlay)
  
  throws IOException 
  {
    ArrayList<LogicalOverlayNetwork> setsOfClusters = new ArrayList<LogicalOverlayNetwork>();
    Iterator<Node> inputIterator = currentQEP.getRT().getRoot().getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      ArrayList<String> trueCluster = new ArrayList<String>();
      LogicalOverlayNetwork curerntOverlay = new LogicalOverlayNetwork();
      curerntOverlay.addClusterNode(currentQEP.getRT().getRoot().getID(), trueCluster);
      setsOfClusters.add(curerntOverlay);
      Node input = inputIterator.next();
      int setIndex = setsOfClusters.size();
      while(setIndex > 0)
      {
        curerntOverlay = setsOfClusters.get(setIndex -1);
        setsOfClusters.remove(curerntOverlay);
        
        alternative(trueCluster, input.getID(), setsOfClusters, curerntOverlay, superLogicalOverlay);
        setIndex--;
      }
    } 
    setsOfClusters = removeDuplicates(setsOfClusters);
    setsOfClusters = clean(setsOfClusters);
    new LogicalOverlayNetworkUtils().storeSetAsTextFile(setsOfClusters, new File(localFolder.toString() + sep + "overLaySets"));
    return setsOfClusters;
  }

  private ArrayList<LogicalOverlayNetwork> clean(ArrayList<LogicalOverlayNetwork> setsOfClusters)
  {
    ArrayList<LogicalOverlayNetwork> cleaned = new ArrayList<LogicalOverlayNetwork>();
    Iterator<LogicalOverlayNetwork> iterator = setsOfClusters.iterator();
    while(iterator.hasNext())
    {
      LogicalOverlayNetwork next = iterator.next();
      if(this.hasCorrectLevelsOfResileince(next,currentQEP.getRT()))
          cleaned.add(next);
    }
    return cleaned;
  }

  /**
   * goes though the set of clusters and looks for duplicates
   * @param setsOfClusters
   */
  private  ArrayList<LogicalOverlayNetwork> removeDuplicates(ArrayList<LogicalOverlayNetwork> setsOfClusters)
  {
    ArrayList<LogicalOverlayNetwork> cleaned = new  ArrayList<LogicalOverlayNetwork>();
    while(setsOfClusters.size() != 0)
    {
      LogicalOverlayNetwork current = setsOfClusters.get(0);
      cleaned.add(current);
      for(int index = setsOfClusters.size() -1; index >= 0; index--)
      {
        if(current.equals(setsOfClusters.get(index)))
        {
          setsOfClusters.remove(index);
        }
      }
    }
    return cleaned;
  }

  /**
   * starts the creation of combinations
   * @param canditateNodes
   * @return
   */
  private ArrayList<ArrayList<String>> createPermutations(ArrayList<String> canditateNodes)
  {
    ArrayList<ArrayList<String>> combinations = new ArrayList<ArrayList<String>>();
    if(canditateNodes.size() != 0)
    {
      int position = 0;
      ArrayList<String> combination = new ArrayList<String>();  
      createPermutation(combinations, combination, position, canditateNodes);
    }
    return combinations;
  }

  /**
   * recursively goes though the collection creating combinations
   * @param combinations
   * @param combination
   * @param position
   * @param canditateNodes 
   */
  private void createPermutation(ArrayList<ArrayList<String>> combinations,
      ArrayList<String> combination, int position, ArrayList<String> canditateNodes)
  {
    if(position == canditateNodes.size())
    {
      ArrayList<String> clonedCombination = cloner.deepClone(combination);
      if(clonedCombination.size() >= k_resilence_level)
        combinations.add(clonedCombination);
    }
    else
    {
      combination.add(canditateNodes.get(position));
      createPermutation(combinations, combination, position+ 1, canditateNodes);
      combination.remove(combination.size()-1);
      createPermutation(combinations, combination, position+ 1, canditateNodes);
    }
  }

  /**
   * recursively creates alternatives
   * @param trueCluster
   * @param id
   * @param setsOfClusters
   * @param superLogicalOverlay 
   * @param curerntOverlay 
   */
  private void alternative(ArrayList<String> parentCluster, String childID,
                           ArrayList<LogicalOverlayNetwork> setsOfClusters, 
                           LogicalOverlayNetwork curerntOverlay,
                           LogicalOverlayNetwork superLogicalOverlay)
  {
    ArrayList<String> childCluster = superLogicalOverlay.getEquivilentNodes(childID);
    ArrayList<String> trueChildCluster = new ArrayList<String>();
    Iterator<String> childClusterItertor = childCluster.iterator();
    while(childClusterItertor.hasNext())
    {
      String childClusterNodeID = childClusterItertor.next();
      if(hasConnections(childClusterNodeID, parentCluster))
      {
        trueChildCluster.add(childClusterNodeID);
      }
    }
    if(routingTree.getSiteTree().getNode(childID).isLeaf())
    {
      curerntOverlay.addClusterNode(childID, trueChildCluster);
      setsOfClusters.remove(curerntOverlay);
      setsOfClusters.add(curerntOverlay);
    }
    else
    {
      ArrayList<ArrayList<String>> combinations = this.createPermutations(trueChildCluster);
      Iterator<ArrayList<String>> combinationIterator = combinations.iterator();
      if(combinations.size() == 0)
      {
        Iterator<Node> childInputs = routingTree.getSiteTree().getNode(childID).getInputsList().iterator();
        while(childInputs.hasNext())
        {
          Node childInput = childInputs.next();
          ArrayList<String> combination = new ArrayList<String>();
          alternative(combination, childInput.getID(),setsOfClusters,curerntOverlay, superLogicalOverlay);
        }
      }
      else
      {
        while(combinationIterator.hasNext())
        {
          ArrayList<String> combination = combinationIterator.next();
          LogicalOverlayNetwork nextOverlay = this.cloneOverlay(curerntOverlay);
          nextOverlay.addClusterNode(childID, combination);
          Iterator<Node> childInputs = routingTree.getSiteTree().getNode(childID).getInputsList().iterator();
          //if going upon another iteration, then add new combination to the set
          boolean first = true;
          while(childInputs.hasNext())
          {
        	if(!first)
        	{
        	  nextOverlay = setsOfClusters.get(setsOfClusters.size() -1);
        	  setsOfClusters.remove(setsOfClusters.size() -1);
        	}
            Node childInput = childInputs.next();
            alternative(combination, childInput.getID(),setsOfClusters,nextOverlay, superLogicalOverlay);
            if(first)
            	first = false;
          }
        }
      }
    }
  }

  /**
   * checks if a node can communicate with a set of other nodes
   * @param childClusterNodeID
   * @param parentCluster
   * @return
   */
  private boolean hasConnections(String nodeId, ArrayList<String> setToLinkTo)
  {
    Iterator<String> setIterator = setToLinkTo.iterator();
    boolean same = true;
    Site compareSite =  network.getSite(nodeId);
    while(setIterator.hasNext() && same)
    {
      String setId = setIterator.next();
      //if the same node is being compared, then remove it from the child
      if(nodeId.equals(setId))
        return false;
      Site setSite = network.getSite(setId);
      if(network.getRadioLink(compareSite, setSite) == null)
        same = false;
    }
    return same;
  }
  
  /**
   * goes though all nodes in topology and compares them to see if they are equivalent 
   * by the use of the localClusterEquivalentRelation
   * @return 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private LogicalOverlayNetwork setUpPhase() 
  throws 
  SchemaMetadataException, 
  TypeMappingException, 
  OptimizationException
  {
    LogicalOverlayNetwork logicalOverlay = new LogicalOverlayNetwork();
    ArrayList<Node> secondNetworkNodes = new ArrayList<Node>(network.getNodes());
    Iterator<Node> firstNodeIterator = currentQEP.getRT().getSiteTree().nodeIterator(TraversalOrder.POST_ORDER);
    while(firstNodeIterator.hasNext())
    {
      Iterator<Node> secondNodeIterator = secondNetworkNodes.iterator();
      Node clusterHead = firstNodeIterator.next();
      if(clusterHead.getOutDegree() != 0)
      {
        while(secondNodeIterator.hasNext())
        {
          Node equilvientNode = secondNodeIterator.next();
          if(LocalClusterSuperEquivalenceRelation.isEquivalent(clusterHead, equilvientNode, currentQEP, network, k_resilence_sense))
          {
            logicalOverlay.addClusterNode(clusterHead.getID(), equilvientNode.getID());
          }
        }
      }
    }
    return logicalOverlay;
  }
  
  /**
   * helper method to generates different types of overlay
   * @param logicalOverlay
   * @return
   */
  private LogicalOverlayNetwork cloneOverlay(LogicalOverlayNetwork logicalOverlay)
  {
    return logicalOverlay.generateClone();
  }
  
}
