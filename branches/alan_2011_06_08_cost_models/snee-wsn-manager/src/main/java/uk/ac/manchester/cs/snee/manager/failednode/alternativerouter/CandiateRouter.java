package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.manager.failednode.metasteiner.MetaSteinerTree;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class CandiateRouter extends Router
{
  private String sep = System.getProperty("file.separator");
  private Topology network;
  private Cloner cloner;
  private File failedChainMain;
  private File outputFolder;
  private File desintatedOutputFolder;
  /**
   * constructor
   * @param network 
   * @param outputFolder 
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   */
  public CandiateRouter(Topology network, File outputFolder) throws NumberFormatException,
      SNEEConfigurationException
  {
    super();
    this.network = network;   
    //Copy connectivity graph so that original never touched.
    cloner = new Cloner();
    cloner.dontClone(Logger.class);
    this.outputFolder = outputFolder;
    failedChainMain = new File(outputFolder.toString() + sep + "chains");
    failedChainMain.mkdir();
  }
  
  /**
   * calculates all routes which replace the failed nodes.
   * @param disconnectedNodes 
   * 
   * @param paf
   * @param queryName
   * @param numberOfRoutingTreesToWorkOn 
   * @return
   * @throws SchemaMetadataException 
   */
  
  public ArrayList<RT> generateRoutes(RT oldRoutingTree, ArrayList<String> failedNodes, 
                                     ArrayList<String> disconnectedNodes, String queryName, Integer numberOfRoutingTreesToWorkOn) throws SchemaMetadataException
  {
    //container for new routeing trees
    ArrayList<RT> newRoutingTrees = new ArrayList<RT>();
    HashMapList<Integer ,Tree> failedNodeToRoutingTreeMapping = new HashMapList<Integer,Tree>();
    /*remove failed nodes from the failed node list, which are parents of a failed node already.
    * this allows routes calculated to be completely independent of other routes */
    HashMapList<Integer ,String> failedNodeLinks = createLinkedFailedNodes(failedNodes, disconnectedNodes, oldRoutingTree);
    
    Iterator<Integer> failedLinkIterator = failedNodeLinks.keySet().iterator();
    int chainCounter = 1;
    //removes excess nodes and edges off the working topolgy, calculates new routes, and adds them to hashmap
    while(failedLinkIterator.hasNext())
    {    
      File chainFolder = new File(failedChainMain.toString() + sep + "chain" + chainCounter);
      chainFolder.mkdir();
      //set up folder to hold alternative routes
      desintatedOutputFolder = new File(chainFolder.toString() + sep + "AllAlternatives");
      desintatedOutputFolder.mkdir();
      Topology workingTopology = cloner.deepClone(network);
      Integer key = failedLinkIterator.next();
      ArrayList<String> setofLinkedFailedNodes = failedNodeLinks.get(key);
      //sources used as a carrier to retrieve all input nodes
      ArrayList<String> sources = new ArrayList<String>();
      String sink = removeExcessNodesAndEdges(workingTopology, oldRoutingTree, setofLinkedFailedNodes, sources);
      
      //output reduced topology for help in keeping track of progress
      workingTopology.exportAsDOTFile(chainFolder.toString() + sep + "reducedtopology");

      //calculate different routes around linked failed site.
      ArrayList<Tree> routesForFailedNode = 
        createRoutes(workingTopology, numberOfRoutingTreesToWorkOn, sources, 
                     sink, oldRoutingTree.getPAF(), oldRoutingTree, chainFolder);
      
      //adds routes to system
      failedNodeToRoutingTreeMapping.addAll(key, routesForFailedNode);
    }
    //merges new routes to create whole entire routingTrees
    newRoutingTrees =  mergeSections(failedNodeToRoutingTreeMapping, oldRoutingTree, 
                                     failedNodes, numberOfRoutingTreesToWorkOn);
    outputcompleteTrees(newRoutingTrees, outputFolder);
    return newRoutingTrees;
  }

  /**
   * outputs all routing trees to autonomic folder
   * @param newRoutingTrees
   * @param outputFolder 
   */
  private void outputcompleteTrees(ArrayList<RT> newRoutingTrees, File outputFolder)
  {
    File output = new File(outputFolder.toString() + sep + "completeRoutingTrees");
    output.mkdir();
    Iterator<RT> routes = newRoutingTrees.iterator();
    int counter = 1;
    while(routes.hasNext())
    {
      RT route = routes.next();
      new RTUtils(route).exportAsDotFile(output.toString() + sep + "completeRoute" + counter);
      new RTUtils(route).exportAsTextFile(output.toString() + sep + "completeRoute" + counter);
      counter ++;
    }
    
  }

  /**
   * merges sections of trees to make full routing trees.
   * @param failedNodeToRoutingTreeMapping
   * @param oldRoutingTree
   * @param failedNodes 
   * @param numberOfRoutingTreesToWorkOn 
   * @return
   */
  private ArrayList<RT> mergeSections(
      HashMapList<Integer, Tree> failedNodeToRoutingTreeMapping,
      RT oldRoutingTree, ArrayList<String> failedNodes, Integer numberOfRoutingTreesToWorkOn)
  {
    int counter = 0;
    ArrayList<RT> newRoutingTrees = new ArrayList<RT>();
    boolean first = true;
    Random randomiser = new Random();
    long max = calcuateMaxTrees(failedNodeToRoutingTreeMapping);
    RT clonedOldRoutingTree = cloner.deepClone(oldRoutingTree);
    removeFailedNodesFromOldRT(clonedOldRoutingTree, failedNodes);
    while(counter < numberOfRoutingTreesToWorkOn && counter < max)
    {

      Iterator<Integer> keyIterator = failedNodeToRoutingTreeMapping.keySet().iterator();
      //get new routing tree
      RT newRoutingTree = cloner.deepClone(clonedOldRoutingTree);
      //for each chain, connect a tree to complete it.
      while(keyIterator.hasNext())
      {
        Tree choice = chooseChoice(keyIterator, failedNodeToRoutingTreeMapping, randomiser, first);
        connectChildAndParent(newRoutingTree, choice);
        updateNodesEdgeArray(choice, newRoutingTree);
      }
      //store new routingTree
      newRoutingTrees.add(newRoutingTree);
      first = false;
      counter ++;
    }
    return newRoutingTrees;
  }

  private void updateNodesEdgeArray(Tree choice, RT newRoutingTree)
  {
  //add extra nodes to new routing table nodes storage, with correct edges
    Iterator<Node> choiceNodeIterator = choice.getNodes().iterator();
    while(choiceNodeIterator.hasNext())
    {
      Node choiceNode = choiceNodeIterator.next();
      if(!this.compareNodeToArray(choiceNode, new ArrayList<Node>(newRoutingTree.getSiteTree().getNodes())))
      {
        newRoutingTree.getSiteTree().addNode(choiceNode);
      }
    } 
  }

  private Tree chooseChoice(Iterator<Integer> keyIterator, HashMapList<Integer, 
                            Tree> failedNodeToRoutingTreeMapping, 
                            Random randomiser, boolean first)
  {
    Integer key = keyIterator.next();
    ArrayList<Tree> choices = failedNodeToRoutingTreeMapping.get(key);
    Tree choice = null;
    if(first)
      choice = choices.get(0);
    else
      choice = choices.get(randomiser.nextInt(choices.size()));
    choices.remove(choice);
    choice = cloner.deepClone(choice);
    return choice;
  }

  private void connectChildAndParent(RT newRoutingTree, Tree choice)
  {
    //connect parent
    Site treeParent =  newRoutingTree.getSite(choice.getRoot().getID());
    Iterator<Node> choiceInputIterator = choice.getRoot().getInputsList().iterator();
    while(choiceInputIterator.hasNext())
    {
      Node choiceChild = choiceInputIterator.next();
      treeParent.addInput(choiceChild);
      choiceChild.removeOutput(choiceChild.getOutput(0));
      choiceChild.addOutput(treeParent);
    }
    //get children of choice.
    Iterator<Node> choiceChildrenIterator = getLeafSourceIterator(choice);//choice.getLeafNodes().iterator();
    //connect children
    while(choiceChildrenIterator.hasNext())
    {
      Node choiceChild = choiceChildrenIterator.next();
      Node choiceParent = choiceChild.getOutput(0);
      Site treeChild =  newRoutingTree.getSite(choiceChild.getID());
      treeChild.addOutput(choiceParent);
      if(choiceChild.getInDegree() > 0)
      {
        Iterator<Node> inputs = choiceChild.getInputsList().iterator();
        while(inputs.hasNext())
        {
          Node input = inputs.next();
          treeChild.addInput(input);
          input.removeOutput(choiceChild);
          input.addOutput(treeChild);
        }
      }
      choiceParent.removeInput(choiceChild);
      choiceParent.addInput(treeChild);
    } 
    newRoutingTree.getSiteTree().updateNodesAndEdgesColls(newRoutingTree.getRoot());
    
  }

  /**
   * goes though tree looking for sources which are not the sink and palces them in a list
   * @param choice
   * @return
   */
  private Iterator<Node> getLeafSourceIterator(Tree choice)
  {
    Iterator<Node> nodeIterator = choice.getNodes().iterator();
    List<Node> sources = new ArrayList<Node>();
    while(nodeIterator.hasNext())
    {
      Node node = nodeIterator.next();
      Site site = (Site) node;
      if(site.isSource() && !site.getID().equals(choice.getRoot().getID()))
        sources.add(node);
    }
    return sources.iterator();
  }

  /**
   * creates a disconnected routing tree, leaving holes to be filled in with calculated trees.
   * @param oldRoutingTree
   * @param failedNodes
   */
  private void removeFailedNodesFromOldRT(RT oldRoutingTree,
      ArrayList<String> failedNodes)
  {
    //iterate over failed nodes removing one by one
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    while(failedNodeIterator.hasNext())
    {
      //get failed node
      Node toRemove = oldRoutingTree.getSite(failedNodeIterator.next());
      //remove input link off parent
      toRemove.getOutput(0).removeInput(toRemove);
      //remove inputExchange from parent
     // removeParentExchange(toRemove);
      //remove output link off each child
      Iterator<Node> childIterator = toRemove.getInputsList().iterator();
      while(childIterator.hasNext())
      {
        childIterator.next().removeOutput(toRemove);
      }
      oldRoutingTree.getSiteTree().removeNode(toRemove.getID());
    }
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
   * calculates what the maxiumum number of rotuign trees can be calculated from the sections.
   * @param failedNodeToRoutingTreeMapping
   * @return
   */
  private long calcuateMaxTrees(
      HashMapList<Integer, Tree> failedNodeToRoutingTreeMapping)
  {
    int items = 0;
    long max = 0;
    int r = failedNodeToRoutingTreeMapping.keySet().size();
    Iterator<Integer> keyIterator = failedNodeToRoutingTreeMapping.keySet().iterator();
    while(keyIterator.hasNext())
    {
      Integer key = keyIterator.next();
      ArrayList<Tree> choices = failedNodeToRoutingTreeMapping.get(key);
      items += choices.size();
    }
    max = (factorial(items) / (factorial(r) * factorial(items - r)));
    return max;
  }

  /**
   * basic factorial formula (as no built in java function)
   * @param n
   * @return
   */
  private long factorial( int n )
  {
      if( n <= 1 )     // base case
          return 1;
      else
          return n * factorial( n - 1 );
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
   */
  private ArrayList<Tree> createRoutes(Topology workingTopology,
      Integer numberOfRoutingTreesToWorkOn, ArrayList<String> sources, String sink, PAF paf, 
      RT oldRoutingTree, File outputFolder)
  {
    ArrayList<Tree> routes = new ArrayList<Tree>();
    Tree steinerTree = null;
    try
    {//checks that global route can generate a route between nodes.
      steinerTree = computeSteinerTree(workingTopology, sink, sources); 
      new RTUtils(new RT(paf, "", steinerTree, workingTopology)).exportAsDotFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1)); 
      new RTUtils(new RT(paf, "", steinerTree, workingTopology)).exportAsTextFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1)); 
      routes.add(steinerTree);
    }
    catch(Exception e)//no routes, hand up empty set
    {
      return routes;
    }
    //container for currently tested heuristics
    ArrayList<HeuristicSet> testedHeuristics = new ArrayList<HeuristicSet>();  
    while(routes.size() < numberOfRoutingTreesToWorkOn)
    {
      HeuristicSet set = collectNextHeuristicSet(workingTopology, testedHeuristics);
      //produce tree for set of heuristics
      MetaSteinerTree treeGenerator = new MetaSteinerTree();
      Tree currentTree = treeGenerator.produceTree(set, sources, sink, workingTopology, paf, oldRoutingTree);
      new RTUtils(new RT(paf, "", steinerTree, workingTopology)).exportAsDotFile(desintatedOutputFolder.toString() + sep + "route" + (routes.size() + 1)); 
      new RTUtils(new RT(paf, "", steinerTree, workingTopology)).exportAsTextFile(desintatedOutputFolder.toString() + sep + "route" + (routes.size() + 1)); 
      routes.add(currentTree);
    }
    routes = removeDuplicates(routes);
    outputCleanedRoutes(routes, outputFolder, paf);
    return routes;
  }

  private HeuristicSet collectNextHeuristicSet(Topology workingTopology, ArrayList<HeuristicSet> testedHeuristics)
  {
    boolean alreadyDone = false;
    //get new set of heuristics
    HeuristicSet set;
    do
    {
      FirstNodeHeuristic phi = FirstNodeHeuristic.RandomEnum();
      SecondNodeHeuristic chi = SecondNodeHeuristic.RandomEnum();
      LinkMatrexChoiceHeuristic psi = LinkMatrexChoiceHeuristic.RandomEnum();
      PenaliseNodeHeuristic omega = PenaliseNodeHeuristic.RandomEnum();
      set = new HeuristicSet(chi, phi, psi, omega, workingTopology);
      alreadyDone = comparison(testedHeuristics, set);
    }while(alreadyDone);
    testedHeuristics.add(set);
    return set;
  }

  private void outputCleanedRoutes(ArrayList<Tree> routes, File outputFolder, PAF paf)
  {
    Iterator<Tree> routeIterator = routes.iterator();
    File cleaned = new File(outputFolder.toString() + sep + "reducedPossible");
    cleaned.mkdir();
    int counter = 0;
    while(routeIterator.hasNext())
    {
      Tree currentTree = routeIterator.next();
      new RTUtils(new RT(paf, "", currentTree, network)).exportAsDotFile(cleaned.toString() + sep + "route" + counter); 
      new RTUtils(new RT(paf, "", currentTree, network)).exportAsTextFile(cleaned.toString() + sep + "route" + counter); 
      counter++;
    }
  }

  /**
   * searches though already chosen heuristics and looks to see if set has been used before
   * @param testedHeuristics
   * @param set
   * @return
   */
  private boolean comparison(ArrayList<HeuristicSet> testedHeuristics,
      HeuristicSet set)
  {
    Iterator<HeuristicSet> setIterator = testedHeuristics.iterator();
    while(setIterator.hasNext())
    {
      HeuristicSet usedSet= setIterator.next();
      if(usedSet.getFirstNodeHeuristic() == set.getFirstNodeHeuristic() &&
         usedSet.getLinkMatrexChoiceHeuristic() == set.getLinkMatrexChoiceHeuristic() &&
         usedSet.getPenaliseNodeHeuristic() == set.getPenaliseNodeHeuristic() &&
         usedSet.getSecondNodeHeuristic() == set.getSecondNodeHeuristic())
        return true;
    }
    return false;
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
   */
  private Tree computeSteinerTree(Topology workingTopology, String sink,
      ArrayList<String> sources)
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
    return computeSteinerTree(workingTopology, intSink, intSources, false);
  }

  /**
   * remove failed nodes from the failed node list, which are parents of a failed node already.
   * this allows routes calculated to be completely independent of other routes 
   * @param failedNodes
   * @param disconnectedNodes 
   * @param oldRoutingTree 
   * @return
   */
  private HashMapList<Integer, String> createLinkedFailedNodes(
      ArrayList<String> failedNodes, ArrayList<String> disconnectedNodes, RT RT)
  {
    HashMapList<Integer, String> failedNodeLinkedList = new HashMapList<Integer, String>();
    int currentLink = 0;
    ArrayList<String> alreadyInLink = new ArrayList<String>();
    failedNodes.addAll(disconnectedNodes);
    Iterator<String> oldFailedNodesIterator = failedNodes.iterator();
    while(oldFailedNodesIterator.hasNext())
    {
      String failedNodeID = oldFailedNodesIterator.next();
      if(!alreadyInLink.contains(failedNodeID))
      {
        Site failedSite = RT.getSite(failedNodeID);
        failedNodeLinkedList.add(currentLink, failedNodeID);
        checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, failedSite, failedNodes, currentLink);
        currentLink++;
      }
    }
    return failedNodeLinkedList;
  }

  /**
   * recursive method, which searches all children and parents looking for failed nodes in a link.
   * @param failedNodeLinkedList
   * @param alreadyInLink
   * @param node
   * @param failedNodes
   * @param currentLink
   */
  private void checkNodesChildrenAndParent(
      HashMapList<Integer, String> failedNodeLinkedList,
      ArrayList<String> alreadyInLink, Node node, 
      ArrayList<String> failedNodes, int currentLink)
  {
    Iterator<Node> childrenIterator = node.getInputsList().iterator();
    while(childrenIterator.hasNext())
    {
      Node child = childrenIterator.next();
      if(failedNodes.contains(child.getID()) && !alreadyInLink.contains(child.getID()))
      {
        alreadyInLink.add(child.getID());
        failedNodeLinkedList.add(currentLink, child.getID());
        checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, child, failedNodes, currentLink);
      }
    }
    Node parent = node.getOutput(0);
    if(failedNodes.contains(parent.getID()) && !alreadyInLink.contains(parent.getID()))
    {
      alreadyInLink.add(parent.getID());
      failedNodeLinkedList.add(currentLink, parent.getID());
      checkNodesChildrenAndParent(failedNodeLinkedList, alreadyInLink, parent, failedNodes, currentLink);
    } 
  }

  /**
   * removes all nodes and edges associated with the old routing tree 
   * apart from the children and parent of a failed node
   * @param workingTopology
   * @param oldRoutingTree
   * @param setofLinkedFailedNodes
   * @param failedNodes 
   */
  private String removeExcessNodesAndEdges(Topology workingTopology,
      RT oldRoutingTree, ArrayList<String> setofLinkedFailedNodes, ArrayList<String> savedChildSites)
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
        if(!setofLinkedFailedNodes.contains(currentChild.getID()))
          savedChildSites.add(currentChild.getID());
      }
      //go though parent
      Node output = failedSite.getOutput(0);
      if(!setofLinkedFailedNodes.contains(output.getID()))
        savedParentSite = output.getID();
    }
    //go though entire old routing tree, removing nodes which are not in the saved sites array
    Iterator<Site> siteIterator = oldRoutingTree.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      if(!savedChildSites.contains(site.getID()) && !savedParentSite.equals(site.getID()))
      {
        workingTopology.removeNodeAndAssociatedEdges(site.getID());
      }
    }
    
    return savedParentSite;
  }

}
