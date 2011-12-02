package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

public class PhysicalToLogicalConversion
{
  
  private LogicalOverlayNetwork logicalOverlay = null;
  private Topology network = null;
  private File localFolder;
  private String sep = System.getProperty("file.separator");
  
  public PhysicalToLogicalConversion(LogicalOverlayNetwork logicalOverlay, 
                                     Topology network,
                                     File localFolder)
  {
    this.logicalOverlay = logicalOverlay;
    this.network = network;
    this.localFolder = localFolder;
  }
  
  /**
   * transfer qep over clusters
   */
  public void transferQEPs() 
  {
    Iterator<String> keys = logicalOverlay.getKeySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      Iterator<String> eqNodeIterator = logicalOverlay.getEquivilentNodes(key).iterator();
      Node clusterHead = network.getNode(key);
      while(eqNodeIterator.hasNext())
      {
        String eqNode = eqNodeIterator.next();
        Node equilvientNode = network.getNode(eqNode);
        //add sites fragments and operaotrs onto equivlent node
        transferSiteQEP(logicalOverlay.getQep(), clusterHead, equilvientNode);
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
    new IOTUtils(qep.getIOT(), qep.getCostParameters()).exportAsDotFileWithFrags(localFolder.toString() + sep + "iotBefore", "iot with eqiv nodes", true);

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
        if(clonedPart.getPrevious() == null)
          rootOp = clonedPart.getSourceFrag().getRootOperator();
        else
          rootOp = clonedPart.getPrevious();
        
        if(!rootOp.getSite().getID().equals(clonedPart.getSite().getID()))
            rootOp = clonedPart;
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
        Site previousSite = null;
        if(part.getPrevious() == null)
          previousSite = qep.getIOT().getRT().getSite(part.getPrevious().getSite().getID());
        else
          previousSite = qep.getIOT().getRT().getSite(part.getPrevious().getSite().getID());
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
  
  
}
