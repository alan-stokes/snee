package uk.ac.manchester.cs.snee.manager.failednode.metasteiner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.HeuristicSet;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class MetaSteinerTree
{
  
  public MetaSteinerTree()
  {
    
  }
  
  /**
   * creates a route based off heuristics contianed in set linking sources with sink 
   * based off the topology working topology.
   * @param set
   * @param sources
   * @param desiredSinkID
   * @param workingTopology
   * @param paf 
   * @param oldRoutingTree 
   * @return
   */
  public Tree produceTree(HeuristicSet set, ArrayList<String> sources, 
      String desiredSinkID, Topology workingTopology, PAF paf, RT oldRoutingTree)
  {
    //create randomiser
    Random randomiser = new Random();
    //create a array which holds all steiner nodes.
    ArrayList<String> bucket = new ArrayList<String>(sources);
    bucket.add(desiredSinkID);
    //create pointer for tree
    MetaSteinerTreeObjectContainer container = null;
    //create pointers for sink 
    String sinkID = null;
    container = chooseFirstNodePlacement(set, sinkID, desiredSinkID, bucket, workingTopology, randomiser);
    while(bucket.size() != 0)
    {
      MetaTopology weightedTopology = new MetaTopology(updateWeighting(workingTopology, container, set));
      selectNodesToLinkTogether(set, bucket, container, weightedTopology, randomiser);
      
      //find route between child and parent.
      Path finalPath = weightedTopology.getShortestPath(container.getChildID(), container.getParentID(), set);
      //link path into tree
      mergePathIntoTree(finalPath, bucket, container);
      //update number of sources in all nodes in steiner tree
      updateNoSources(container.getSteinerTree(), oldRoutingTree);
    }
    if(!container.getSteinerTree().getRoot().getID().equals(desiredSinkID))
    {
      //rotate tree so that root operator is the sink node
      rotateTree(container.getSteinerTree().getNode(desiredSinkID), container.getSteinerTree().getNode(desiredSinkID), container.getSteinerTree());
      container.getSteinerTree().setRoot(container.getSteinerTree().getNode(desiredSinkID));
    }
    return container.getSteinerTree();
  }
  
  /**
   * update all nodes in tree in post order so that correct values used.
   * @param steinerTree
   * @param oldRoutingTree 
   */
  private void updateNoSources(Tree steinerTree, RT oldRoutingTree)
  {
    Iterator<Site> siteIterator = steinerTree.nodeIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site currentSite = siteIterator.next();
      if(!(currentSite.getInDegree() == 0))
      {
        int runningTotal = 0;
        Iterator<Node> inputIterator = currentSite.getInputsList().iterator();
        while(inputIterator.hasNext())
        {
          Site input = (Site) inputIterator.next();
          Site oldInput = oldRoutingTree.getSite(input.getID());
          if(oldInput == null)
            runningTotal += input.getNumSources();
          else
            runningTotal += oldInput.getNumSources();
        }
        currentSite.setNoSources(runningTotal);
      }
      
    }
    
  }

  /**
   * if the tree is rooted at a node not the sink, change edges so that they make sink the root
   * @param tree 
   * @param steinerTree
   * @param sink
   */
  private void rotateTree(Node current, Node prevNode, Tree tree)
  {
    for (int n=0; n<current.getInDegree(); n++) 
    {
       Site inputNode = (Site)current.getInput(n);
       //Do nothing, edge has right direction
       rotateTree(inputNode, current, tree);
    }
    for (int n=0; n<current.getOutDegree(); n++) 
    {
      Site outputNode = (Site)current.getOutput(n);
      if (outputNode == prevNode) 
      {
        continue;
      }
      reverseEdgeDirection(current, outputNode, tree);
      rotateTree(outputNode, current, tree);
    }
  }
  
  /**
   * reverse inputs and outputs
   * @param source
   * @param dest
   */
  public void reverseEdgeDirection(Node source, Node dest, Tree tree)
  {
    source.removeOutput(dest);
    dest.removeInput(source);
    source.addInput(dest);
    dest.addOutput(source);
    //change the edge for the sourceNode
    Iterator<EdgeImplementation> edgeIterator = tree.getNodeEdges(source.getID()).iterator();
    while(edgeIterator.hasNext())
    {
      EdgeImplementation edge = edgeIterator.next();
      if(edge.getSourceID().equals(source.getID()) && 
         edge.getDestID().equals(dest.getID()))
      {
        tree.removeEdge(source, dest);
        tree.addEdge(dest, source);
      }
    }
    
  }

  /**
   * links the path into the steiner tree
   * @param finalPath
   * @param steinerTree
   * @param bucket
   * @param childID
   * @param parentID
   */
  private void mergePathIntoTree(Path finalPath, ArrayList<String> bucket, MetaSteinerTreeObjectContainer container)
  {
    Site childNode = new Site(container.getChildID());
    container.getSteinerTree().addNode(childNode);
    bucket.remove(container.getChildID());
    //add path into steinterTree
    Iterator<Site> siteIterator = finalPath.iterator();
    //collect first site
    Site firstSite = siteIterator.next();
    if(!treeContainsSiteID(firstSite.getID(),  container.getSteinerTree()))
    {
      firstSite = new Site(firstSite);
      container.getSteinerTree().addNode(firstSite);
    }
    else
    {
      firstSite = (Site) container.getSteinerTree().getNode(firstSite.getID());
    }
    boolean connected = false;
    while(siteIterator.hasNext() && !connected)
    {
      Site secondSite = siteIterator.next();
      if(!treeContainsSiteID(secondSite.getID(),  container.getSteinerTree()))
      {
        Site newSecond = new Site(secondSite);
        newSecond.addInput(firstSite);
        firstSite.addOutput(newSecond);
        container.getSteinerTree().addNode(newSecond);
        container.getSteinerTree().addEdge(firstSite, newSecond);
        
        firstSite = newSecond;
      }
      else
      {
        connected = true;
        Node node =  container.getSteinerTree().getNode(secondSite.getID());
        node.addInput(firstSite);
        firstSite.addOutput(node);
        
      }
    }
    
  }

  /**
   * chooses two nodes to connect together based off the second heuristic. these nodes are stored in the 
   * child and parent id's passed to the method as parameters.
   * @param set
   * @param bucket
   * @param steinerTree
   * @param childID
   * @param parentID
   * @param weightedTopology
   * @param randomiser
   */
  private void selectNodesToLinkTogether(HeuristicSet set,
      ArrayList<String> bucket, MetaSteinerTreeObjectContainer container, 
      MetaTopology weightedTopology, Random randomiser)
  {
  //select next 
    switch(set.getSecondNodeHeuristic())
    {
      case CLOSEST_SINK:
        Iterator<String> bucketIterator = bucket.iterator();
        container.setParentID(container.getSteinerTree().getRoot().getID());
        double currentChildCost = 0;
        boolean first = true;
        //go though each node in bucket looking for the one closest to the sink
        while(bucketIterator.hasNext())
        {
          String nodeID = bucketIterator.next();
          Path path = weightedTopology.getShortestPath(nodeID, container.getParentID(), set);
          //find cost of new path
          double cost = new MetaPath(path).getCost(weightedTopology, set);
          //compare current cost with best cost
          if(first)
          {
            container.setChildID(nodeID);
            currentChildCost = cost;
            first = false;
          }
          else if(currentChildCost > cost)
          {
            container.setChildID(nodeID);
            currentChildCost = cost;
          }
        }
      break;
      case CLOSEST_ANY:
        bucketIterator = bucket.iterator();
        currentChildCost = 0;
        first = true;
        //go though each node in bucket looking for the one closest to the sink
        while(bucketIterator.hasNext())
        {
          String currentChildID = bucketIterator.next();
          Iterator<Node> treeNodesIterator = new ArrayList<Node>(container.getSteinerTree().getNodes()).iterator();
          while(treeNodesIterator.hasNext())
          {
            String currentParentID = treeNodesIterator.next().getID();
            Path path = weightedTopology.getShortestPath(currentChildID, currentParentID, set);
            //find cost of new path
            double cost = new MetaPath(path).getCost(weightedTopology, set);
            //compare current cost with best cost
            if(first)
            {
              container.setChildID(currentChildID);
              container.setParentID(currentParentID);
              currentChildCost = cost;
              first = false;
            }
            else if(currentChildCost > cost)
            {
              container.setChildID(currentChildID);
              container.setParentID(currentParentID);
              currentChildCost = cost;
            }
          }
        }
      break;
      case RANDOM:
        container.setChildID(bucket.get(randomiser.nextInt(bucket.size())));
        bucket.remove(container.getChildID());
        ArrayList<Node> nodesInTree =  new ArrayList<Node>(container.getSteinerTree().getNodes());
        container.setParentID(nodesInTree.get(randomiser.nextInt(nodesInTree.size())).getID());
      break;
    }
    
  }

  /**
   * choose first node to place into the steinerTree
   * @param set
   * @param sinkID
   * @param sink
   * @param bucket
   * @param workingTopology
   * @param randomiser
   */
  private MetaSteinerTreeObjectContainer chooseFirstNodePlacement(HeuristicSet set, String sinkID,
      String sink, ArrayList<String> bucket, Topology workingTopology, Random randomiser)
  {
    Tree steinerTree = null;
    //choose first node to add as root 
    switch(set.getFirstNodeHeuristic())
    {
      case SINK:
        sinkID = sink;
        steinerTree = new Tree(new Site((Site)workingTopology.getNode(sinkID)), true);
        bucket.remove(sink);
      break;
      case RANDOM:
        
        int  randomIndex = randomiser.nextInt(bucket.size());
        sinkID = bucket.get(randomIndex);
        steinerTree = new Tree(new Site((Site)workingTopology.getNode(sinkID)), true);
        bucket.remove(randomIndex);
      break;
    }
    MetaSteinerTreeObjectContainer container = new MetaSteinerTreeObjectContainer(steinerTree);
    return container;
    
  }

  /**
   * looks for a specific node by id in the tree, returns true if it exists, false otherwise
   * @param id
   * @param tree
   * @return
   */
  private boolean treeContainsSiteID(String id, Tree tree)
  {
    Iterator<Node> siteIterator = tree.getNodes().iterator();
    while(siteIterator.hasNext())
    {
      Node node = siteIterator.next();
      if(node.getID().equals(id))
        return true;
    }
    return false;
  }

  /**
   * Updates weightings in topology to fit heuristics in set
   * @param workingTopology
   * @param container
   * @param set 
   * @return
   */
  private Topology updateWeighting(Topology workingTopology, MetaSteinerTreeObjectContainer container, 
                                   HeuristicSet set)
  {
    switch(set.getPenaliseNodeHeuristic())
    {
    case TRUE:
      Cloner cloner = new Cloner();
      cloner.dontClone(Logger.class);
      Topology weightedTopology = cloner.deepClone(workingTopology);
      Iterator<Node> nodeIterator = weightedTopology.getNodes().iterator();
      while(nodeIterator.hasNext())
      {
        Site node = (Site) nodeIterator.next();
        Site steinerTreeNode = (Site) container.getSteinerTree().getNode(node.getID());
        double noSources = 0;
        //get correct count of sources
        if(steinerTreeNode == null)
          noSources = node.getNumSources();
        else
          noSources = steinerTreeNode.getNumSources();
        
        HashSet<EdgeImplementation> edges = weightedTopology.getNodeEdges(node.getID());
        Iterator<EdgeImplementation> edgeIterator = edges.iterator();
        while(edgeIterator.hasNext())
        {
          EdgeImplementation edgeImp = edgeIterator.next();
          Site sourceSite = weightedTopology.getSite(edgeImp.getSourceID());
          Site destinationSite = weightedTopology.getSite(edgeImp.getDestID());
          RadioLink link = weightedTopology.getRadioLink(sourceSite, destinationSite);
          switch(set.getEdgeChoice(edgeImp.getID()))
          {
            case ENERGY:
              link.setEnergyCost(Math.pow(link.getEnergyCost() + 1, noSources + 4));
            break;
            case LATENCY:
              link.setLatencyCost(Math.pow(link.getLatencyCost() + 1, noSources + 4));
          }    
        }
      }
      return weightedTopology;

    case FALSE:
      return workingTopology;
    }
    return null;
  }
  
}
