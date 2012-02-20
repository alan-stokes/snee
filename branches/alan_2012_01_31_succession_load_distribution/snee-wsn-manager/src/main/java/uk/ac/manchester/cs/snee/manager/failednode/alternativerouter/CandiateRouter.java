package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.manager.failednode.metasteiner.MetaSteinerTree;
import uk.ac.manchester.cs.snee.manager.failednode.metasteiner.MetaSteinerTreeException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class CandiateRouter extends Router
{
  private String sep = System.getProperty("file.separator");
  private Topology network;
  private PAF paf;
  private SourceMetadataAbstract _metadataManager;
  private Cloner cloner;
  private File failedChainMain;
  private File outputFolder;
  private File desintatedOutputFolder;
  private File chainFolder;
  private List<HeuristicSet> heuristics = new ArrayList<HeuristicSet>();
  private int heuristicsPosition = 0;
  /**
   * constructor
   * @param network 
   * @param outputFolder 
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   */
  public CandiateRouter(Topology network, File outputFolder, PAF paf, 
                        SourceMetadataAbstract _metadataManager) 
  throws NumberFormatException, SNEEConfigurationException
  {
    super();
    this.network = network;  
    this.paf = paf;
    this._metadataManager = _metadataManager;
    //Copy connectivity graph so that original never touched.
    cloner = new Cloner();
    cloner.dontClone(Logger.class);
    this.outputFolder = outputFolder;
    boolean successor = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR);
    if(!successor)
    {
      failedChainMain = new File(outputFolder.toString() + sep + "chains");
      failedChainMain.mkdir();
    }
    setupHeuristicsets(1, new HeuristicSet());
  }
  
  /**
   * recursive method to set up sets
   * @throws SNEEConfigurationException 
   */
  private void setupHeuristicsets(int position, HeuristicSet set)
  throws SNEEConfigurationException
  {
    switch(position)
    {
      case 1:
        while(FirstNodeHeuristic.hasNext())
        {
          set.setFirstNodeHeuristic(FirstNodeHeuristic.next());
          setupHeuristicsets(position+ 1, set);
        }
        FirstNodeHeuristic.resetCounter();
      break;        
      case 2:
        while(SecondNodeHeuristic.hasNext())
        {
          set.setSecondNodeHeuristic(SecondNodeHeuristic.next());
          setupHeuristicsets(position+ 1, set);
        }
        SecondNodeHeuristic.resetCounter();
      break;
      case 3:
        while(PenaliseNodeHeuristic.hasNext())
        {
          set.setPenaliseNodeHeuristic(PenaliseNodeHeuristic.next());
          setupHeuristicsets(position+ 1, set);
        }
        PenaliseNodeHeuristic.resetCounter();
      break;
      case 4:
        while(SensibleHeuristic.hasNext())
        {
          set.setSensibleHeuristic(SensibleHeuristic.next());
          setupHeuristicsets(position+ 1, set);
        }
        SensibleHeuristic.resetCounter();
      break;
      case 5:
        while(LinkMatrexChoiceHeuristic.hasNext())
        {
          set.setLinkMatrexChoiceHeuristic(LinkMatrexChoiceHeuristic.next());
          heuristics.add(new HeuristicSet(set.getSecondNodeHeuristic(), 
                                          set.getFirstNodeHeuristic(), 
                                          set.getLinkMatrexChoiceHeuristic(),
                                          set.getPenaliseNodeHeuristic(),
                                          set.getSensibleHeuristic()));
        }
        LinkMatrexChoiceHeuristic.resetCounter();
      break;
      default:
      break;
    }
    
  }

  /**
   * calculates all routes which replace the failed nodes.
   * @param depinnedNodes 
   * 
   * @param paf
   * @param queryName
   * @param numberOfRoutingTreesToWorkOn 
   * @return
   * @throws SchemaMetadataException 
   * @throws MetaSteinerTreeException 
   * @throws RouterException 
   * @throws IOException 
   */
  
  public ArrayList<RT> generateCompleteRouteingTrees(RT oldRoutingTree, ArrayList<String> failedNodes, 
                                     ArrayList<String> depinnedNodes, String queryName, 
                                     Integer numberOfRoutingTreesToWorkOn)                                
  throws 
  SchemaMetadataException, MetaSteinerTreeException, RouterException, IOException
  {
    //container for new routeing trees
    ArrayList<RT> newRoutingTrees = new ArrayList<RT>();
    HashMapList<Integer ,Tree> failedNodeToRoutingTreeMapping = new HashMapList<Integer,Tree>();
    /*remove failed nodes from the failed node list, which are parents of a failed node already.
    * this allows routes calculated to be completely independent of other routes */
    HashMapList<Integer ,String> failedNodeLinks =
                          generateSetsOfLinkedFailedNodes(failedNodes, oldRoutingTree, depinnedNodes);
    
    Iterator<Integer> failedLinkIterator = failedNodeLinks.keySet().iterator();
    int chainCounter = 1;
    //removes excess nodes and edges off the working topolgy, calculates new routes, and adds them to hashmap
    while(failedLinkIterator.hasNext())
    {    
      chainFolder = new File(failedChainMain.toString() + sep + "chain" + chainCounter);
      chainFolder.mkdir();
      //set up folder to hold alternative routes
      desintatedOutputFolder = new File(chainFolder.toString() + sep + "AllAlternatives");
      desintatedOutputFolder.mkdir();
      Topology workingTopology = cloner.deepClone(network);
      Integer key = failedLinkIterator.next();
      ArrayList<String> setofLinkedFailedNodes = failedNodeLinks.get(key);
      //sources used as a carrier to retrieve all input nodes
      ArrayList<String> sources = new ArrayList<String>();
      String sink = removeExcessNodesAndEdges(workingTopology, oldRoutingTree, setofLinkedFailedNodes, 
                                              sources, depinnedNodes);
      System.out.println("sources are : " + sources.toString() + " and sink is: " + sink);
      //output reduced topology for help in keeping track of progress  
      new CandiateRouterUtils(this.network).exportReducedTopology(chainFolder, true, workingTopology);
      new CandiateRouterUtils(this.network).exportDepinnedNodes(depinnedNodes, chainFolder);
      //calculate different routes around linked failed site.
      ArrayList<Tree> routesForFailedNode = 
        startOfRouteGeneration(workingTopology, numberOfRoutingTreesToWorkOn, sources, 
                               sink, oldRoutingTree.getPAF(), oldRoutingTree, chainFolder);
      
      //adds routes to system
      failedNodeToRoutingTreeMapping.addAll(key, routesForFailedNode);
      chainCounter++;
    }
    //merges new routes to create whole entire routingTrees
    newRoutingTrees =  mergeRoutingTreesFragments(failedNodeToRoutingTreeMapping, oldRoutingTree, 
                                     failedNodes, numberOfRoutingTreesToWorkOn, depinnedNodes);
    new CandiateRouterUtils(network).exportCompleteTrees(newRoutingTrees, outputFolder);
    return newRoutingTrees;
  }

  /**
   * merges sections of trees to make full routing trees.
   * @param failedNodeToRoutingTreeMapping
   * @param oldRoutingTree
   * @param failedNodes 
   * @param numberOfRoutingTreesToWorkOn 
   * @param disconnectedNodes 
   * @return
   */
  private ArrayList<RT> mergeRoutingTreesFragments(
                                      HashMapList<Integer, Tree> failedNodeToRoutingTreeMapping,
                                      RT oldRoutingTree, ArrayList<String> failedNodes, 
                                      Integer numberOfRoutingTreesToWorkOn, 
                                      ArrayList<String> disconnectedNodes)
  {
    ArrayList<RT> newRoutingTrees = new ArrayList<RT>();
    try
    {
    RT clonedOldRoutingTree = cloner.deepClone(oldRoutingTree);
    removeFailedNodesFromOldRT(clonedOldRoutingTree, failedNodes, disconnectedNodes);
    //setup patched rotuing tree as a topology, so that routing can be operated over it to generate true tree
    Topology oldRoutingTreeAsToplogy = new Topology("tempRoutingTopology", true);
    oldRoutingTreeAsToplogy.mergeGraphsUsingSites(clonedOldRoutingTree.getSiteTree(), network);
    //merge patch trees together to topolgoy tree.
    ArrayList<Integer> keys = new ArrayList<Integer>(failedNodeToRoutingTreeMapping.keySet());
    mergeRoutingTreeFragmentsRecursively(keys, failedNodeToRoutingTreeMapping, oldRoutingTreeAsToplogy,
                                         newRoutingTrees, 0);
    return newRoutingTrees;
    }
    catch(Exception e)
    {
      System.out.println("something died in the recursive mergement of routing tree fragments: " + e.getMessage());
      e.printStackTrace();
      return newRoutingTrees; 
    }
  }

  /**
   * recursively merges route fragments till no more mixes are possible
   * @param failedNodeToRoutingTreeMapping
   * @param failedNodeToRoutingTreeMapping 
   * @param clonedOldRoutingTree
   * @param newRoutingTrees
   * @param i 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws RouterException 
   * @throws NumberFormatException 
   * @throws SchemaMetadataException 
   */
  private void mergeRoutingTreeFragmentsRecursively(
      ArrayList<Integer> keys, HashMapList<Integer, Tree> failedNodeToRoutingTreeMapping, 
      Topology oldRoutingTreeAsToplogy, ArrayList<RT> newRoutingTrees, int position) 
  throws OptimizationException, NumberFormatException, 
  RouterException, SNEEConfigurationException, SchemaMetadataException
  {
    
    if(position < keys.size() -1)
    {
      ArrayList<Tree> routingTreeFragments =  failedNodeToRoutingTreeMapping.get(keys.get(position));
      Iterator<Tree> treeFragmentIterator = routingTreeFragments.iterator();
      while(treeFragmentIterator.hasNext())
      {
        Tree treeFragment = treeFragmentIterator.next();
        oldRoutingTreeAsToplogy.mergeGraphsUsingSites(treeFragment, network);
        mergeRoutingTreeFragmentsRecursively(keys, failedNodeToRoutingTreeMapping, oldRoutingTreeAsToplogy,
                                             newRoutingTrees, position+1);
      }
    }
    else
    {
      ArrayList<Tree> routingTreeFragments =  failedNodeToRoutingTreeMapping.get(keys.get(position));
      Iterator<Tree> treeFragmentIterator = routingTreeFragments.iterator();
      while(treeFragmentIterator.hasNext())
      {
        Tree treeFragment = treeFragmentIterator.next();
        oldRoutingTreeAsToplogy.mergeGraphsUsingSites(treeFragment, network);
        new CandiateRouterUtils(oldRoutingTreeAsToplogy).exportTempRoutingTopology(outputFolder, "tempTopology" + newRoutingTrees.size() + 1, true, oldRoutingTreeAsToplogy);
        RT newTree = new Router().doRouting(paf, "newTree", oldRoutingTreeAsToplogy, _metadataManager);
        newRoutingTrees.add(newTree);
      }
    }   
  }

  /**
   * creates a disconnected routing tree, leaving holes to be filled in with calculated trees.
   * @param oldRoutingTree
   * @param failedNodes
   * @param disconnectedNodes 
   */
  private void removeFailedNodesFromOldRT(RT oldRoutingTree,
      ArrayList<String> failedNodes, ArrayList<String> disconnectedNodes)
  {
    ArrayList<String> allNodes = new ArrayList<String> ();
    //allNodes.addAll(disconnectedNodes);
    allNodes.addAll(failedNodes);
    
    //iterate over failed nodes removing one by one
    Iterator<String> failedNodeIterator = allNodes.iterator();
    while(failedNodeIterator.hasNext())
    {
      //get failed node
      Node toRemove = oldRoutingTree.getSite(failedNodeIterator.next());
      //remove input link off parent
      toRemove.getOutput(0).removeInput(toRemove);
      //remove output link off each child
      Iterator<Node> childIterator = toRemove.getInputsList().iterator();
      while(childIterator.hasNext())
      {
        childIterator.next().removeOutput(toRemove);
      }
      oldRoutingTree.getSiteTree().removeNode(toRemove.getID());
    }
    /*
    Iterator<String> disconnectedNodesIterator = disconnectedNodes.iterator();
    while(disconnectedNodesIterator.hasNext())
    {
      String nodeID = disconnectedNodesIterator.next();
      Node toClean = oldRoutingTree.getSite(nodeID);
      oldRoutingTree.getSiteTree().removeSiteEdges(nodeID);
      if(toClean.getOutDegree() != 0)
        toClean.getOutput(0).removeInput(toClean);
      Iterator<Node> childIterator = toClean.getInputsList().iterator();
      while(childIterator.hasNext())
      {
        childIterator.next().removeOutput(toClean);
      }
      toClean.clearInputs();
      toClean.clearOutputs();
    }*/
    
    //clear all operators off sites
    Iterator<Node> nodeIterator = oldRoutingTree.getSiteTree().getNodes().iterator();
    while(nodeIterator.hasNext())
    {
      Site node = (Site) nodeIterator.next();
      node.clearInstanceExchangeComponents();
      node.clearExchangeComponents();
    }
  }
  
  /**
   * method which creates at maximum numberOfRoutingTreesToWorkOn routes if possible. 
   * First uses basic steiner-tree algorithm to determine if a route exists between 
   * sources and sink. if a route exists then, try for numberOfRoutingTreesToWorkOn 
   * iterations with different heuristics. if no route after basic, then returns null. 
   * @param workingTopology
   * @param numberOfRoutingTreesToWorkOn
   * @param sources
   * @param sink
   * @param paf 
   * @param oldRoutingTree 
   * @param outputFolder 
   * @return
   * @throws MetaSteinerTreeException 
   * @throws RouterException 
   */
  private ArrayList<Tree> startOfRouteGeneration(Topology workingTopology,
      Integer numberOfRoutingTreesToWorkOn, ArrayList<String> sources, String sink, PAF paf, 
      RT oldRoutingTree, File outputFolder) 
  throws
  MetaSteinerTreeException, RouterException
  {
    ArrayList<Tree> routes = new ArrayList<Tree>();
    Tree steinerTree = null;
    //checks that global route can generate a route between nodes.
    steinerTree = computeSteinerTree(workingTopology, sink, sources, paf); 
    new CandiateRouterUtils(workingTopology).exportRouteFragments(desintatedOutputFolder, routes, paf, steinerTree);
    routes.add(steinerTree);
    //container for currently tested heuristics
    heuristicsPosition = 0;
    return generateRoutesRecursively(routes, workingTopology, sources, sink, paf, oldRoutingTree);
  }

  /**
   * recursive method which tries to generate routes based off heuristics, 
   * catches exceptions to be pushed back into the system
   * @param routes
   * @param workingTopology
   * @param sources
   * @param sink
   * @param paf
   * @param oldRoutingTree
   * @return
   */
  private ArrayList<Tree> generateRoutesRecursively(ArrayList<Tree> routes, 
                                              Topology workingTopology, ArrayList<String> sources, 
                                              String sink, PAF paf, RT oldRoutingTree)
  {
    try
    {
      while(heuristicsPosition < heuristics.size() -1)
      {
        HeuristicSet set = collectNextHeuristicSet(workingTopology);
        //produce tree for set of heuristics
        MetaSteinerTree treeGenerator = new MetaSteinerTree();
        Tree currentTree = treeGenerator.produceTree(set, sources, sink, workingTopology, paf, oldRoutingTree);
        new CandiateRouterUtils(workingTopology).exportRouteFragments(desintatedOutputFolder, routes, paf, currentTree);
        routes.add(currentTree);
      }
      routes = removeDuplicates(routes);
      new CandiateRouterUtils(network).exportSavedRoutes(routes, chainFolder, paf);
      return routes;
    }
    catch(Exception e)
    {
      return generateRoutesRecursively(routes, workingTopology, sources, sink, paf, 
          oldRoutingTree);
    }
    
  }

  /**
   * gets next heusristic set
   * @param workingTopology
   * @return
   */
  private HeuristicSet collectNextHeuristicSet(Topology workingTopology)
  {
    HeuristicSet set = heuristics.get(heuristicsPosition);
    heuristicsPosition++;
    set.setup(workingTopology);
    return set;
  }

  /**
   * removes all routes which are duplicates from routes.
   * @param routes
   */
  private ArrayList<Tree> removeDuplicates(ArrayList<Tree> routes)
  {
    Tree [] temporaryArray = new Tree[routes.size()];
    routes.toArray(temporaryArray);
    for(int templateIndex = 0; templateIndex < routes.size(); templateIndex++)
    {
      Tree template = temporaryArray[templateIndex];
      if(template != null)
      {
        for(int compareIndex = 0; compareIndex < routes.size(); compareIndex++)
        {
          if(compareIndex != templateIndex)
          {
            Tree compare = temporaryArray[compareIndex];
            if(compare != null)
            {
              ArrayList<Node> templateNodes = new ArrayList<Node>(template.getNodes());
              ArrayList<Node> compareNodes = new ArrayList<Node>(compare.getNodes());
              if(templateNodes.size() == compareNodes.size())
              {
                boolean equal = true;
                Iterator<Node> templateIterator = templateNodes.iterator();
                Iterator<Node> compareIterator = compareNodes.iterator();
                while(templateIterator.hasNext())
                {
                  Node currentNode = templateIterator.next();
                  if(!compareNodeToArray(currentNode, compareNodes))
                    equal = false;
                    
                }
                if(equal)
                {
                  while(compareIterator.hasNext())
                  {
                    Node currentNode = compareIterator.next();
                    if(!compareNodeToArray(currentNode, templateNodes))
                      equal = false;
                  }
                  if(equal)
                  {
                    temporaryArray[compareIndex] = null; 
                  }
                }
              }
            } 
          }
        }
      }
    }   
    routes = new ArrayList<Tree>();
    for(int tempIndex = 0; tempIndex < temporaryArray.length; tempIndex++)
    {
      if(temporaryArray[tempIndex] != null)
        routes.add(temporaryArray[tempIndex]);
    }
    return routes;
  }
  
  /**
   * compares a node with an array of nodes, if the node exists, return true
   * @param currentNode
   * @param compareNodes
   * @return
   */
  private boolean compareNodeToArray(Node currentNode,
      ArrayList<Node> compareNodes)
  {
    for(int index = 0; index < compareNodes.size(); index++)
    {
      if(currentNode.getID().equals(compareNodes.get(index).getID()))
        return true;
    }
    return false;
  }

  /**
   * method used to convert between string input and int input used by basic router method
   * @param workingTopology
   * @param sink
   * @param sources
   * @return
   * @throws RouterException 
   */
  private Tree computeSteinerTree(Topology workingTopology, String sink,
      ArrayList<String> sources, PAF paf) 
  throws RouterException
  {
    int intSink = Integer.parseInt(sink);
    int [] intSources = new int[sources.size()];
    Iterator<String> sourceIterator = sources.iterator();
    int counter = 0;
    while(sourceIterator.hasNext())
    {
      intSources[counter] = Integer.parseInt(sourceIterator.next());
      counter++;
    }
    
    Tree steinerTree =  computeSteinerTree(workingTopology, intSink, intSources, false);
    MetaSteinerTree.finalCheckForSourceNodes(steinerTree, sources, paf);
    return steinerTree;
  }

  /**
   * remove failed nodes from the failed node list, which are parents of a failed node already.
   * this allows routes calculated to be completely independent of other routes 
   * @param failedNodes
   * @param disconnectedNodes 
   * @param oldRoutingTree 
   * @return
   */
  private HashMapList<Integer, String> generateSetsOfLinkedFailedNodes(
      ArrayList<String> failedNodes, RT RT, ArrayList<String> disconnectedNodes)
  {
    ArrayList<String> combinedNodes = new ArrayList<String>();
    combinedNodes.addAll(failedNodes);
    combinedNodes.addAll(disconnectedNodes);
    
    HashMapList<Integer, String> failedNodeLinkedList = new HashMapList<Integer, String>();
    int currentLink = 0;
    ArrayList<String> alreadyInLink = new ArrayList<String>();
    Iterator<String> oldFailedNodesIterator = combinedNodes.iterator();
    while(oldFailedNodesIterator.hasNext())
    {
      String failedNodeID = oldFailedNodesIterator.next();
      if(!alreadyInLink.contains(failedNodeID))
      {
        Site failedSite = RT.getSite(failedNodeID);
        failedNodeLinkedList.add(currentLink, failedNodeID);
        alreadyInLink.add(failedNodeID);
        alreadyInLink = checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, failedSite, combinedNodes, currentLink);
        currentLink++;
      }
    }
    System.out.println("numebr of keys is " + failedNodeLinkedList.keySet().size());
    return failedNodeLinkedList;
  }

  /**
   * recursive method, which searches all children and parents looking for failed nodes in a link.
   * @param failedNodeLinkedList
   * @param alreadyInLink
   * @param node
   * @param failedNodes
   * @param currentLink
   * @return 
   */
  private ArrayList<String> checkNodesChildrenAndParent(
      HashMapList<Integer, String> failedNodeLinkedList,
      ArrayList<String> alreadyInLink, Node node, 
      ArrayList<String> failedNodes, int currentLink)
  {
    if(node.getInDegree() != 0)
    {
      Iterator<Node> childrenIterator = node.getInputsList().iterator();
      while(childrenIterator.hasNext())
      {
        Node child = childrenIterator.next();
        if(failedNodes.contains(child.getID()) && !alreadyInLink.contains(child.getID()))
        {
          alreadyInLink.add(child.getID());
          checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, child, failedNodes, currentLink);
        }
      }
    }
    Node parent = node.getOutput(0);
    if(failedNodes.contains(parent.getID()) && !alreadyInLink.contains(parent.getID()))
    {
      alreadyInLink.add(parent.getID());
      failedNodeLinkedList.add(currentLink, parent.getID());
      checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, parent, failedNodes, currentLink);
    } 
    return alreadyInLink;
  }

  /**
   * removes all nodes and edges associated with the old routing tree 
   * apart from the children and parent of a failed node
   * @param workingTopology
   * @param oldRoutingTree
   * @param setofLinkedFailedNodes
   * @param failedNodes 
   */
  private String removeExcessNodesAndEdges(Topology workingTopology, RT oldRoutingTree, 
                                           ArrayList<String> setofLinkedFailedNodes, 
                                           ArrayList<String> savedChildSites,
                                           ArrayList<String> depinnedNodes)
  {
    String savedParentSite = "";
    //locate all children of all failed nodes in link which are active, and place them into saved sites
    Iterator<String> linkedFailedNodes = setofLinkedFailedNodes.iterator();
    while(linkedFailedNodes.hasNext())
    {
      String nodeId = linkedFailedNodes.next();
      Site failedSite = oldRoutingTree.getSite(nodeId);
      Iterator<Node> inputs = failedSite.getInputsList().iterator();
      //go though inputs
      while(inputs.hasNext())
      {
        Node currentChild = inputs.next();
        if((!setofLinkedFailedNodes.contains(currentChild.getID()) && 
           !depinnedNodes.contains(currentChild.getID())) 
        ||
           (oldRoutingTree.getSite(currentChild.getID()).isSource()) &&
           !setofLinkedFailedNodes.contains(currentChild.getID()))
          savedChildSites.add(currentChild.getID());
      }
      //go though parent
      Node output = failedSite.getOutput(0);
      if((!setofLinkedFailedNodes.contains(output.getID()) &&
         !depinnedNodes.contains(output.getID())) 
      ||
        (oldRoutingTree.getSite(output.getID()).isSource() &&
        !setofLinkedFailedNodes.contains(output.getID())))
        
        savedParentSite = output.getID();
    }
    
    // locate any source nodes which are in the depinned nodes
    Iterator<String> depinnedNodesIterator = depinnedNodes.iterator();
    while(depinnedNodesIterator.hasNext())
    {
      String depinnedNode = depinnedNodesIterator.next();
      if(!savedChildSites.contains(depinnedNode) && !savedParentSite.equals(depinnedNode) &&
          oldRoutingTree.getSite(depinnedNode).isSource())
      {
        savedChildSites.add(depinnedNode);
      }
    }
    
    //go though entire old routing tree, removing nodes which are not in the saved sites array
    Iterator<Site> siteIterator = oldRoutingTree.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      if(!savedChildSites.contains(site.getID()) && !savedParentSite.equals(site.getID()) && 
         !depinnedNodes.contains(site.getID()))
      {
        workingTopology.removeNodeAndAssociatedEdges(site.getID());
      }
    }
    return savedParentSite;
  }

  public ArrayList<RT> generateAlternativeRoutingTrees(String queryName)
  {
    ArrayList<RT> routes = new ArrayList<RT>();
    ArrayList<Tree> trees = new ArrayList<Tree>();
    generateRoutesRecursively(trees);
    Iterator<Tree> treeIterator = trees.iterator();
    while(treeIterator.hasNext())
    {
      Tree tree = treeIterator.next();
      routes.add(new RT(paf,queryName, tree, network));
    }
    return routes;
  }

  private ArrayList<Tree> generateRoutesRecursively(ArrayList<Tree> routes)
  {
    try
    {
      while(heuristicsPosition < heuristics.size() -1)
      {
        Topology workingTopology = cloner.deepClone(this.network);
        HeuristicSet set = collectNextHeuristicSet(workingTopology);
        //produce tree for set of heuristics
        MetaSteinerTree treeGenerator = new MetaSteinerTree();
        SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) 
        paf.getDLAF().getSources().iterator().next();
        int sink = sm.getGateway(); 
        int[] sources = sm.getSourceSites(paf);
        //convert between int and string
        ArrayList<String> sourcesString = new ArrayList<String>();
        for(int intIndex = 0; intIndex < sources.length; intIndex++)
        {
          sourcesString.add(new Integer(sources[intIndex]).toString());
        }
        String sinkString = new Integer(sink).toString();
        Tree currentTree = treeGenerator.produceTree(set, sourcesString, sinkString, workingTopology, paf);
        routes.add(currentTree);
      }
      routes = removeDuplicates(routes);
      return routes;
    }
    catch(Exception e)
    {
      return generateRoutesRecursively(routes);
    }
    
  }

}
