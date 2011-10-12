package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.StrategyID;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalCluster;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalClusterUtils;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LocalClusterEquivalenceRelation;
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
public class FailedNodeStrategyLocal extends FailedNodeStrategyAbstract
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -7562607134737502147L;
  private Topology network = null;
  private FailedNodeLocalCluster clusters;
  private File localFolder;
  private String sep = System.getProperty("file.separator");
  /**
   * constructor
   * @param autonomicManager
   */
  public FailedNodeStrategyLocal(AutonomicManager autonomicManager, SourceMetadataAbstract _metadata)
  {
    super(autonomicManager, _metadata); 
    setupFolders(outputFolder);
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
    clusters = new FailedNodeLocalCluster();
    network = getWsnTopology();
    locateEquivalentNodes();
    new FailedNodeLocalClusterUtils(clusters, localFolder).outputAsTextFile();
  }

  private void setupFolders(File outputFolder)
  {
    localFolder = new File(outputFolder.toString() + sep + "localStrategy");
    localFolder.mkdir();
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
      Node clusterHead = firstNodeIterator.next();
      while(secondNodeIterator.hasNext())
      {
        Node equilvientNode = secondNodeIterator.next();
        if(LocalClusterEquivalenceRelation.isEquivalent(clusterHead, equilvientNode, qep, network))
        {
          clusters.addClusterNode(clusterHead.getID(), equilvientNode.getID());
          //add sites fragments and operaotrs onto equivlent node
          transferSiteQEP(qep, clusterHead, equilvientNode);
        }
      }
    }
  }
  
  /**
   * clones operators onto new site, so that when the iot is called, they should work correctly
   * @param qep
   * @param clusterHead
   * @param equilvientNode
   */
  private void transferSiteQEP(SensorNetworkQueryPlan qep, Node clusterHead,
                           Node equilvientNode)
  {
    Site equilvientSite = (Site) equilvientNode;
    Site clusterHeadSite = (Site) clusterHead;
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    //set up iot with new operators 
    ArrayList<InstanceOperator> ClusterHeadsiteInstanceOperators = 
                qep.getIOT().getOpInstances(clusterHeadSite, TraversalOrder.PRE_ORDER, true);
    Iterator<InstanceOperator> siteInstanceOperatorsIterator = 
                ClusterHeadsiteInstanceOperators.iterator();
    
    InstanceFragment firstFrag = new InstanceFragment();
    while(siteInstanceOperatorsIterator.hasNext())
    {
      InstanceOperator operator = siteInstanceOperatorsIterator.next();
      InstanceOperator clonedOperator = cloner.deepClone(operator);
      clonedOperator.setSite(equilvientSite);
      if(!clonedOperator.getCorraspondingFragment().getID().equals(firstFrag.getID()))
      {
        firstFrag = new InstanceFragment(clonedOperator.getCorraspondingFragment().getID());
        firstFrag.setSite(equilvientSite);
        firstFrag.addOperator(clonedOperator);
        firstFrag.setRootOperator(clonedOperator);
        qep.getIOT().addInstanceFragment(firstFrag);
      }
      else
      {
        firstFrag.addOperator(clonedOperator);
      }
      qep.getIOT().addOpInstToSite(clonedOperator, equilvientSite);
    }   
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

  private void rewireRoutingTree(String failedNodeID, String equivilentNodeID, RT currentRoutingTree) 
  throws 
  OptimizationException
  {
    Topology network = this.getWsnTopology();
    network.removeNodeAndAssociatedEdges(failedNodeID);
    Node equivilentNode = network.getNode(equivilentNodeID);
    Node failedNode = currentRoutingTree.getSiteTree().getNode(failedNodeID);
    currentRoutingTree.getSiteTree().replaceNode(failedNode, equivilentNode);
  }

  private void rewireNodes(IOT clonedIOT, String failedNodeID, String equivilentNodeID)
  {
    ArrayList<Node> children = clonedIOT.getInputSites(qep.getRT().getSite(failedNodeID));
    Iterator<Node> chidlrenIterator = children.iterator();
    while(chidlrenIterator.hasNext())
    {
      Node child = chidlrenIterator.next();
      InstanceOperator rootOp = clonedIOT.getRootOperatorOfSite((Site) child);
      InstanceExchangePart childExchange = (InstanceExchangePart) rootOp;
      //if exchanges end at the ffailed node, realter all exhcanges in path to follow to new site
      if(childExchange.getDestSite().getID().equals(failedNodeID))
        childExchange.setDestinitionSite(network.getSite(equivilentNodeID));
     
      ArrayList<InstanceExchangePart> exchanges = 
        clonedIOT.getExchangeOperators(network.getSite(equivilentNodeID));
      Iterator<InstanceExchangePart> exchangeIterator = exchanges.iterator();
      while(exchangeIterator.hasNext())
      {
        InstanceExchangePart exchange = exchangeIterator.next();
        if(exchange.getPrevious().equals(exchange))
        {
          childExchange.setNextExchange(exchange);
        }
      }
      InstanceExchangePart equivilentNodesRootExchange = 
        (InstanceExchangePart) clonedIOT.getRootOperatorOfSite(network.getSite(equivilentNodeID));
      Node parent = clonedIOT.getNode(qep.getRT().getSite(failedNodeID).getID()).getOutput(0);
      exchanges = clonedIOT.getExchangeOperators((Site) parent);
      exchangeIterator = exchanges.iterator();
      while(exchangeIterator.hasNext())
      {
        InstanceExchangePart exchange = exchangeIterator.next();
        if(exchange.getPrevious().getCurrentSite().getID().equals(failedNodeID))
        {
          exchange.setPrev(equivilentNodesRootExchange);
        }
      }
    }
  }
  
  
  @Override
  public List<Adaptation> adapt(ArrayList<String> failedNodeIDs) 
  throws OptimizationException
  {
    System.out.println("Running Failed Node FrameWork Local");
    List<Adaptation> adapatation = new ArrayList<Adaptation>();
    Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
    Adaptation adapt = new Adaptation(qep, StrategyID.FailedNodeLocal, 1);
    
    IOT clonedIOT = cloner.deepClone(qep.getIOT());
    RT currentRoutingTree = cloner.deepClone(qep.getRT());
    while(failedNodeIDsIterator.hasNext())
    {
      String failedNodeID = failedNodeIDsIterator.next();
      String equivilentNodeID = retrieveNewClusterHead(failedNodeID);
      adapt.addActivatedSite(equivilentNodeID);
      rewireRoutingTree(failedNodeID, equivilentNodeID, currentRoutingTree);
      //rewire children
      rewireNodes(clonedIOT, failedNodeID, equivilentNodeID);
    }
    try
    {
      
      PAF pinnedPaf = this.pinPhysicalOperators(clonedIOT, failedNodeIDs, new ArrayList<String>());
      InstanceWhereSchedular instanceWhere = 
        new InstanceWhereSchedular(pinnedPaf, currentRoutingTree, qep.getCostParameters(), localFolder.toString());
      IOT newIOT = instanceWhere.getIOT();
      //run new iot though when scheduler and locate changes
      AgendaIOT newAgenda = doSNWhenScheduling(newIOT, qep.getQos(), qep.getID(), qep.getCostParameters());
      //output new and old agendas
      new FailedNodeStrategyLocalUtils(this).outputAgendas(newAgenda, qep.getAgendaIOT(), 
                                                           qep.getIOT(), newIOT, localFolder);
      
      boolean success = assessQEPsAgendas(qep.getIOT(), newIOT, qep.getAgendaIOT(), newAgenda, 
                                          false, adapt, failedNodeIDs, currentRoutingTree);
      
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
}
