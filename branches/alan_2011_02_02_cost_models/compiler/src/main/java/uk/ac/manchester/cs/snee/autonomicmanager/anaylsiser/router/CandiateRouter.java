package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class CandiateRouter extends Router
{

  /**
   * constructor
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   */
  public CandiateRouter() throws NumberFormatException,
      SNEEConfigurationException
  {
    super();
  }
  
  /**
   * calculates all routes which replace the failed nodes.
   * 
   * @param paf
   * @param queryName
   * @param numberOfRoutingTreesToWorkOn 
   * @return
   */
  
  public ArrayList<RT> findAllRoutes(RT oldRoutingTree, ArrayList<String> failedNodes, 
                                     String queryName, Integer numberOfRoutingTreesToWorkOn)
  {
    //container for new routeing trees
    ArrayList<RT> newRoutingTrees = new ArrayList<RT>();
    HashMapList<Integer ,Tree> failedNodeToRoutingTreeMapping = new HashMapList<Integer,Tree>();
    
    //get connectivity graph
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) oldRoutingTree.getPAF().getDLAF().getSource();
    Topology network = sm.getTopology();
    //Copy connectivity graph so that original never touched.
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    /*remove failed nodes from the failed node list, which are parents of a failed node already.
    * this allows routes calculated to be completely independent of other routes */
    HashMapList<Integer ,String> failedNodeLinks = createLinkedFailedNodes(failedNodes, oldRoutingTree);
    
    Iterator<Integer> failedLinkIterator = failedNodeLinks.keySet().iterator();
    //removes excess nodes and edges off the working topolgy, calculates new routes, and adds them to hashmap
    while(failedLinkIterator.hasNext())
    {
      Topology workingTopology = cloner.deepClone(network);
      Integer key = failedLinkIterator.next();
      ArrayList<String> setofLinkedFailedNodes = failedNodeLinks.get(key);
      ArrayList<String> sources = new ArrayList<String>();
      String sink = removeExcessNodesAndEdges(workingTopology, oldRoutingTree, setofLinkedFailedNodes, sources);
      try
      {
        workingTopology.exportAsDOTFile("workingtopology");
      }
      catch (SchemaMetadataException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      //calculate different routes around linked failed site.
      ArrayList<Tree> routesForFailedNode = createRoutes(workingTopology, numberOfRoutingTreesToWorkOn, sources, sink);
      failedNodeToRoutingTreeMapping.addAll(key, routesForFailedNode);
    }
    //merges new routes to create whole entire routingTrees

    //score them and place in descending order.
    return new ArrayList<RT>(newRoutingTrees.subList(0, numberOfRoutingTreesToWorkOn));
  }

  /**
   * method which creates at maximum numberOfRoutingTreesToWorkOn routes if possible. 
   * First uses basic steiner-tree algorithm to determine if a route exists between sources and sink.
   * if a route exists then, try for numberOfRoutingTreesToWorkOn iterations with different heuristics. 
   * if no route after basic, then returns null. 
   * @param workingTopology
   * @param numberOfRoutingTreesToWorkOn
   * @param sources
   * @param sink
   * @return
   */
  private ArrayList<Tree> createRoutes(Topology workingTopology,
      Integer numberOfRoutingTreesToWorkOn, ArrayList<String> sources, String sink)
  {
    ArrayList<Tree> routes = new ArrayList<Tree>();
    Tree steinerTree = computeSteinerTree(workingTopology, sink, sources); 
    //if no route exists between sink and childs. return empty array.
    if(steinerTree == null)
      return routes;
    
      
    while(routes.size() < numberOfRoutingTreesToWorkOn)
    {
      Phi phi = Phi.RandomEnum();
      Chi chi = Chi.RandomEnum();
      Psi psi = Psi.RandomEnum();
      Omega omega = Omega.RandomEnum();

      Tree currentTree = metaSteinerTree(phi, chi, psi, omega, sources, sink, workingTopology);
      routes.add(currentTree);
    }
    removeDuplicates(routes);
    return routes;
  }


  /**
   * remvoes all routes which are duplicates from routes.
   * @param routes
   */
  private void removeDuplicates(ArrayList<Tree> routes)
  {
    // TODO Auto-generated method stub
    
  }

  /**
   * creates a route based off heuristics phi,chi,psi,omega linking sources with sink 
   * based off the topology working topology.
   * @param phi
   * @param chi
   * @param psi
   * @param omega
   * @param sources
   * @param sink
   * @param workingTopology
   * @return
   */
  private Tree metaSteinerTree(Phi phi, Chi chi, Psi psi, Omega omega,
      ArrayList<String> sources, String sink, Topology workingTopology)
  {
    //create randomiser
    Random randomiser = new Random();
    //create a array which holds all steiner nodes.
    ArrayList<String> bucket = new ArrayList<String>(sources);
    bucket.add(sink);
    //create pointer for tree
    Tree steinerTree = null;
    //choose first node to add as root 
    switch(phi)
    {
      case SINK:
        Site sinkSite = workingTopology.getSite(sink);
        steinerTree = new Tree(sinkSite, true);
        bucket.remove(sink);
      break;
      case RANDOM:
        int  randomIndex = randomiser.nextInt(bucket.size());
        Site randomSink = workingTopology.getSite(bucket.get(randomIndex));
        steinerTree = new Tree(randomSink, true);
        bucket.remove(randomIndex);
      break;
    }
    
    return null;
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
    return computeSteinerTree(workingTopology, intSink, intSources);
  }

  /**
   * remove failed nodes from the failed node list, which are parents of a failed node already.
   * this allows routes calculated to be completely independent of other routes 
   * @param failedNodes
   * @param oldRoutingTree 
   * @return
   */
  private HashMapList<Integer, String> createLinkedFailedNodes(
      ArrayList<String> failedNodes, RT RT)
  {
    HashMapList<Integer, String> failedNodeLinkedList = new HashMapList<Integer, String>();
    int currentLink = 0;
    ArrayList<String> alreadyInLink = new ArrayList<String>();
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
        workingTopology.removeNode(site.getID());
    }
  return savedParentSite;
  }

}
