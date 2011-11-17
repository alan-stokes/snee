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
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalCluster;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.FailedNodeLocalClusterUtils;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LocalClusterEquivalenceRelation;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

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
    this.currentQEP = (SensorNetworkQueryPlan) oldQep;
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
   * used to update stragies where to output data files
   * @param outputFolder
   */
  public void updateFrameWorkStorage(File outputFolder)
  {
    this.outputFolder = outputFolder;
    setupFolders(outputFolder);
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
          if(LocalClusterEquivalenceRelation.isEquivalent(clusterHead, equilvientNode, currentQEP, network))
          {
            clusters.addClusterNode(clusterHead.getID(), equilvientNode.getID());
            //add sites fragments and operaotrs onto equivlent node
            transferSiteQEP(currentQEP, clusterHead, equilvientNode);
          }
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
    new IOTUtils(qep.getIOT(), this.currentQEP.getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotBefore", "iot with eqiv nodes", true);

    Site equilvientSite = (Site) equilvientNode;
    Site clusterHeadSite = (Site) clusterHead;
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    //set up iot with new operators 
    ArrayList<InstanceOperator> ClusterHeadsiteInstanceOperators = 
                qep.getIOT().getOpInstances(clusterHeadSite, TraversalOrder.PRE_ORDER, true);

    //removes all nodes but the root operators from the collection 
    ArrayList<InstanceOperator> rootOperators = 
      operatorReduction(ClusterHeadsiteInstanceOperators, clusterHeadSite);
    ArrayList<InstanceOperator> clonedRootOperators = new ArrayList<InstanceOperator>();
    
    //clones the root operators (this means all inputs and outputs within the site are now correct
    Iterator<InstanceOperator> clonedRootOperatorIterator = rootOperators.iterator();
    while(clonedRootOperatorIterator.hasNext())
    {
      InstanceOperator rootOp = clonedRootOperatorIterator.next();
      clonedRootOperators.add(cloner.deepClone(rootOp));
    }
    //go though each root operator, correcting site info and 
    //checking if it has a fragment, if so make new fragment and add them to it.
    //then go though each input
    clonedRootOperatorIterator = clonedRootOperators.iterator();
    Iterator<InstanceOperator> rootOperatorIterator = rootOperators.iterator();
    
    while(clonedRootOperatorIterator.hasNext())
    {
      InstanceOperator clonedRootOp = clonedRootOperatorIterator.next();
      InstanceOperator rootOp = rootOperatorIterator.next();
      if(!(rootOp.getSensornetOperator() instanceof SensornetDeliverOperator))
      {
        qep.getIOT().assign(clonedRootOp, equilvientSite);
        InstanceExchangePart clonedPart = (InstanceExchangePart) clonedRootOp; 
        equilvientSite.addInstanceExchangePart(clonedPart);
        InstanceExchangePart part = (InstanceExchangePart) rootOp;
        
        InstanceExchangePart outPart = part.getNext();
        clonedPart.setNextExchange(outPart);
        clonedPart.clearOutputs();
        clonedPart.addOutput(outPart);
  
        clonedPart.setDestFrag(part.getDestFrag());
        clonedPart.getSourceFrag().setSite(equilvientSite); 
        rootOp = clonedPart.getSourceFrag().getRootOperator();
      }
      
      sortOutChildren(rootOp, equilvientSite, clusterHeadSite, qep);
      sortOutFragments(clonedRootOp, equilvientSite, qep, clonedRootOp.getCorraspondingFragment());
    }  
    
  }
  
  /**
   * goes though all operators looking for new fragments, they are then installed into the iot
   * @param rootOp
   * @param equilvientSite
   * @param qep
   */
  private void sortOutFragments(InstanceOperator inOp, Site equilvientSite,
      SensorNetworkQueryPlan qep, InstanceFragment frag)
  {
     ArrayList<Node> inputs = new ArrayList<Node>();
     inputs.addAll(inOp.getInputsList());
     Iterator<Node> nodeIterator = inputs.iterator();
     while(nodeIterator.hasNext())
     {
       InstanceOperator input = (InstanceOperator) nodeIterator.next();
       if(input.getSite().getID().equals(equilvientSite.getID()))
       {
         if(input.getCorraspondingFragment() != null)
         {
           if(frag != null)
           {
             if(!frag.getFragID().equals(input.getCorraspondingFragment().getFragID()))
             {
               qep.getIOT().addInstanceFragment(input.getCorraspondingFragment());
             }
             sortOutFragments(input, equilvientSite, qep, input.getCorraspondingFragment());
           }
           else
           {
             qep.getIOT().addInstanceFragment(input.getCorraspondingFragment());
             sortOutFragments(input, equilvientSite, qep, input.getCorraspondingFragment());
           }
         }
         else
         {
           sortOutFragments(input, equilvientSite, qep, input.getCorraspondingFragment());
         }
       }
       
     }
     
    
  }

  /**
   * takes a root child of a fragment and change its site
   * @param operator
   * @param qep 
   */
  private void sortOutChildren(InstanceOperator operator, Site equilvientSite, Site clusterHeadSite, 
                               SensorNetworkQueryPlan qep)
  { 
    
    qep.getIOT().assign(operator, equilvientSite);
    if(!(operator instanceof InstanceExchangePart))
    {
      operator.getCorraspondingFragment().setSite(equilvientSite);
    }
    
    Iterator<Node> inputIterator = operator.getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      InstanceOperator op = (InstanceOperator) inputIterator.next();
      if(op.getSite().getID().equals(clusterHeadSite.getID()))
      {
        sortOutChildren(op, equilvientSite, clusterHeadSite, qep);
      }
      else
      {
        operator.replaceInput(op, qep.getIOT().getOperatorInstance(op.getID()));
        InstanceExchangePart part = (InstanceExchangePart) operator;
        equilvientSite.addInstanceExchangePart(part);
        Site previousSite = qep.getIOT().getRT().getSite(part.getPrevious().getSite().getID());
        Iterator<InstanceExchangePart> previousSitesExchanges = previousSite.getInstanceExchangeComponents().iterator();
        while(previousSitesExchanges.hasNext())
        {
          InstanceExchangePart previousSitePart = previousSitesExchanges.next();
          if(previousSitePart.getID().equals(part.getPrevious().getID()))
          {
            part.setPreviousExchange(previousSitePart);
            part.clearInputs();
            part.addInput(previousSitePart);
          }
        }
      }
    }
  }

  /**
   * goes though a list of operators and traverses their inputs removing them from the list
   * this is to ensure what is left in the list is the individual root instances for the site
   */
  private ArrayList<InstanceOperator> operatorReduction(
      ArrayList<InstanceOperator> clusterHeadsiteInstanceOperators,
      Site clusterHeadSite)
  {
    ArrayList<InstanceOperator> reducedOperators = new ArrayList<InstanceOperator>();
    reducedOperators.addAll(clusterHeadsiteInstanceOperators);
    
    Iterator<InstanceOperator> clusterOperatorIterator = clusterHeadsiteInstanceOperators.iterator();
    while(clusterOperatorIterator.hasNext())
    {
      InstanceOperator op = clusterOperatorIterator.next();
      InstanceOperator opOutput = null;
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart inop = (InstanceExchangePart) op;
        if(inop.getNext() == null)
          reducedOperators = removeFromCollection(op, reducedOperators);
        else
        {
          opOutput = (InstanceOperator) inop.getNext();
          if(opOutput.getSite().getID().equals(clusterHeadSite.getID()))
            reducedOperators = removeFromCollection(op, reducedOperators);
        } 
      }
      else
      {
        if(op.getOutDegree() != 0)
        {
          opOutput = (InstanceOperator) op.getOutput(0);
          if(opOutput.getSite().getID().equals(clusterHeadSite.getID()))
            reducedOperators = removeFromCollection(op, reducedOperators);
        }
      }
    }
    return reducedOperators;
  }
  
  /**
   * located a node and removes it from a given collection
   * @param op
   * @param collection
   */
  private ArrayList<InstanceOperator> removeFromCollection(InstanceOperator op,
      ArrayList<InstanceOperator> collection)
  {
    ArrayList<InstanceOperator> returned = new ArrayList<InstanceOperator>();
    returned.addAll(collection);
    int index = 0;
    
    Iterator<InstanceOperator> collectionIterator = collection.iterator();
    while(collectionIterator.hasNext())
    {
      InstanceOperator collectionOp = collectionIterator.next();
      if(collectionOp.getID().equals(op.getID()))
        returned.remove(index);
      index++;
    }
    return returned;
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
    this.currentQEP = (SensorNetworkQueryPlan) newQEP;
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
              nextPart.clearInputs();
              nextPart.addInput(eqPart);
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
    try
    {
      System.out.println("Running Failed Node FrameWork Local");
      List<Adaptation> adapatation = new ArrayList<Adaptation>();
      if(this.canAdaptToAll(failedNodeIDs))
      {
        Iterator<String> failedNodeIDsIterator = failedNodeIDs.iterator();
        Adaptation adapt = new Adaptation(currentQEP, StrategyIDEnum.FailedNodeLocal, 1);
      
        IOT clonedIOT = cloner.deepClone(currentQEP.getIOT());
        RT currentRoutingTree = clonedIOT.getRT();
        while(failedNodeIDsIterator.hasNext())
        {
          String failedNodeID = failedNodeIDsIterator.next();
          String equivilentNodeID = retrieveNewClusterHead(failedNodeID);
          //sort out adaptation data structs.
          adapt.addActivatedSite(equivilentNodeID);
          Iterator<Node> redirectedNodesIterator = this.currentQEP.getRT().getSite(failedNodeID).getInputsList().iterator();
          while(redirectedNodesIterator.hasNext())
          {
            adapt.addRedirectedSite(redirectedNodesIterator.next().getID());
          }
          //rewire routing tree
          rewireRoutingTree(failedNodeID, equivilentNodeID, currentRoutingTree);
          //rewire children
          rewireNodes(clonedIOT, failedNodeID, equivilentNodeID);
        }
        new IOTUtils(clonedIOT, this.currentQEP.getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iot", "iot with eqiv nodes", true);
        
        new IOTUtils(clonedIOT, this.currentQEP.getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotInputs", "iot with eqiv nodes", true, true);
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
        
          boolean success = assessQEPsAgendas(currentQEP.getIOT(), newIOT, currentQEP.getAgendaIOT(), newAgendaIOT, newAgenda, 
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
}
