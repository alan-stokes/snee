package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.FrameWorkAbstract;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalCluster;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalClusterUtils;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LocalClusterEquivalenceRelation;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

/**
 * 
 * @author alan
 *class which encapsulates the local framework using clusters and equivalence relations
 */
public class FailedNodeFrameWorkLocal extends FrameWorkAbstract
{
  private Topology network = null;
  private FailedNodeLocalCluster clusters;
  /**
   * constructor
   * @param autonomicManager
   */
  public FailedNodeFrameWorkLocal(AutonomicManager autonomicManager, SourceMetadataAbstract _metadata)
  {
    super(autonomicManager, _metadata); 
  }
	
  /**
   * sets up framework by detecting equivalent nodes and placing them in a cluster
   * @param oldQep
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  public void initilise(QueryExecutionPlan oldQep, Integer noTrees) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException
  {  
    this.qep = (SensorNetworkQueryPlan) oldQep;
    outputFolder = manager.getOutputFolder();
    clusters = new FailedNodeLocalCluster();
    network = getWsnTopology();
    locateEquivalentNodes();
    new FailedNodeLocalClusterUtils(clusters, this.outputFolder).outputAsTextFile();
  }

  /**
   * goes though all nodes in topology and compares them to see if they are equivalent 
   * by the use of the localClusterEquivalentRelation
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private void locateEquivalentNodes() 
  throws 
  SchemaMetadataException, 
  TypeMappingException, 
  OptimizationException
  {
    ArrayList<Node> secondNetworkNodes = new ArrayList<Node>(network.getNodes());
    Iterator<Node> firstNodeIterator = qep.getRT().getSiteTree().nodeIterator(TraversalOrder.POST_ORDER);
    while(firstNodeIterator.hasNext())
    {
      Iterator<Node> secondNodeIterator = secondNetworkNodes.iterator();
      Node firstNode = firstNodeIterator.next();
      while(secondNodeIterator.hasNext())
      {
        Node secondNode = secondNodeIterator.next();
        if(LocalClusterEquivalenceRelation.isEquivalent(firstNode, secondNode, qep, network))
        {
          clusters.addClusterNode(firstNode.getID(), secondNode.getID());
          //add sites fragments and operaotrs onto equivlent node
          transferQEP(qep, firstNode, secondNode);
        }
      }
    }
  }
  
  private void transferQEP(SensorNetworkQueryPlan qep, Node firstNode,
      Node secondNode)
  {
    //TODO develop way to get code onto equivalent nodes (possibly got numerous versions to place)
    Site secondSite = (Site) secondNode;
    ArrayList<InstanceOperator> siteOperators = qep.getIOT().get.getOpInstances(secondSite);
    
  }

  /**
   * used to recalculate clusters based off other adaptations
   * @param newQEP
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  public void reclaculateClusters(QueryExecutionPlan newQEP) 
  throws 
  SchemaMetadataException, 
  TypeMappingException, 
  OptimizationException
  {
    this.qep = (SensorNetworkQueryPlan) newQEP;
    clusters = new FailedNodeLocalCluster();
    locateEquivalentNodes();
  }
  
  /**
   * checks if a cluster exists for a specific node
   * @param primary (cluster head)
   * @return true if cluster exists, false otherwise
   */
  public boolean isThereACluster(String primary)
  {
    if(clusters.getEquivilentNodes(primary).size() != 0)
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
  public String retrieveNewClusterHead(String primary)
  {
    if(isThereACluster(primary))
    {
      String newClusterHead =  clusters.getEquivilentNodes(primary).get(0);
      clusters.removeNode(newClusterHead);
      return newClusterHead;
    }
    else
      return null;
  }

  @Override
  public boolean canAdapt(String failedNode)
  {
    return isThereACluster(failedNode);
  }
  
  @Override
  public boolean canAdaptToAll(ArrayList<String> failedNodes)
  {
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    boolean success = true;
    while(failedNodeIterator.hasNext() && success)
    {
      if(!canAdapt(failedNodeIterator.next()))
        success = false;
    }
    return success;
  }

  @Override
  public List<Adapatation> adapt(ArrayList<String> failedNodeIDs)
  {
    List<Adapatation> adapatation = new ArrayList<Adapatation>();
    Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
    Adapatation adapt = new Adapatation(qep);
    while(failedNodeIDsIterator.hasNext())
    {
      String failedNodeID = failedNodeIDsIterator.next();
      String newNodeID = retrieveNewClusterHead(failedNodeID);
      adapt.addActivatedSite(network.getSite(newNodeID));
      ArrayList<Node> children = qep.getIOT().getInputSites(qep.getRT().getSite(failedNodeID));
      Iterator<Node> chidlrenIterator = children.iterator();
      while(chidlrenIterator.hasNext())
      {
        Node child = chidlrenIterator.next();
        
      }
    }
    adapatation.add(adapt);
    return adapatation;
  }

  
  
  
}
