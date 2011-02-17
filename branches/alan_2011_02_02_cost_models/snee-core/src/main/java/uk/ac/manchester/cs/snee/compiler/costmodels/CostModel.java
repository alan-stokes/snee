package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class CostModel
{
  private Agenda agenda;
  private RT routingTree;
  private InstanceDAF instanceDAF;
  
  private PAF paf;
  
  public CostModel()
  {}

  public float runCardinality() throws OptimizationException 
  {
    long beta = agenda.getBufferingFactor();
	  InstanceOperator rootOperator = instanceDAF.getRoot();
	  Site rootSite = routingTree.getRoot();
	  return rootOperator.getInstanceCardinality(rootSite, instanceDAF, beta);
  }

  public void buildInstance(PAF paf, RT routingTree) 
  throws SNEEException, SchemaMetadataException, OptimizationException
  {
    this.paf = paf;
    this.routingTree = routingTree;
    //generate floating operators / fixed locations
    generatePartialDaf();
    //produce image output so that can be validated
    instanceDAF.exportAsDOTFile("/home/S06/stokesa6/SNEE-TestOutput/partialDAF.dot");
    //do heuristic placement
    doInstanceOperatorSiteAssignment();
    instanceDAF.exportAsDOTFile("/home/S06/stokesa6/SNEE-TestOutput/siteAssignment.dot");
    //remove instances which are redundant
    removeRedundantOpInstances();
    instanceDAF.exportAsDOTFile("/home/S06/stokesa6/SNEE-TestOutput/completeInstanceDAF.dot");
  }

  private void doInstanceOperatorSiteAssignment()
  {
    //iterate over operators looking for ones which haven't got a fixed location
    Iterator<InstanceOperator> InstanceOperatorIterator 
        = instanceDAF.iterator(TraversalOrder.POST_ORDER);
    while(InstanceOperatorIterator.hasNext())
    {
      InstanceOperator instance = InstanceOperatorIterator.next();      
      //if site = null, not been assigned a site yet
      if(instance.getSite() == null)
      {
        if (instance.getInstanceOperator().isAttributeSensitive()) 
        {
          //Attribute-sensitive operator
          assignAttributeSensitiveOpInstances(instance);     
        }
        else if (instance.getInstanceOperator().isRecursive()) 
        {
          //Iterative operator
          assignIterativeOpInstances(instance);
        } 
        else 
        {
          //Other operators
          assignOtherOpTypeInstances(instance);
        }
      } 
    }
  }

  private void assignOtherOpTypeInstances(InstanceOperator instance)
  {
    //find the instance of the child operator and place on same site
    InstanceOperator childOpInst = (InstanceOperator)instance.getInput(0);
    Site opSite = childOpInst.getSite();
    instanceDAF.assign(instance, opSite);
  }

  private void assignIterativeOpInstances(InstanceOperator instance)
  {
    // TODO Auto-generated method stub
    
  }

  private void assignAttributeSensitiveOpInstances(InstanceOperator instance)
  {
    
    // TODO Auto-generated method stub
    
  }

  private void generatePartialDaf() 
  throws SNEEException, SchemaMetadataException
  {
    //make new instance daf
    instanceDAF = new InstanceDAF(routingTree, paf.getQueryName());
    HashMapList<String,InstanceOperator> disconnectedOpInstMapping =
      new HashMapList<String,InstanceOperator>();
    //collect a iterator for physical operators 
    Iterator<SensornetOperator> opIter = paf.operatorIterator(TraversalOrder.POST_ORDER);
    /*iterate though physical operators looking at each and determining 
    how many instances there should be */
    while (opIter.hasNext())
    {
      SensornetOperator op = opIter.next();
      System.out.println(op.getOperatorName());
      
      if (   op instanceof SensornetAcquireOperator 
          || op instanceof SensornetDeliverOperator) 
      {
        //Location-sensitive operator
        addLocationSensitiveOpInstances(op, disconnectedOpInstMapping);
      } 
      else if (op.isAttributeSensitive()) 
      {
        //Attribute-sensitive operator
        addAttributeSensitiveOpInstances(op, disconnectedOpInstMapping);     
      }
      else if (op.isRecursive()) 
      {
        //Iterative operator
        addIterativeOpInstances(op, disconnectedOpInstMapping);
      } 
      else 
      {
        //Other operators
        addOtherOpTypeInstances(op, disconnectedOpInstMapping);
      }
        
      if (op instanceof SensornetDeliverOperator) 
      {
        InstanceOperator opInst = instanceDAF.getOpInstances(op).get(0);
        instanceDAF.setRoot(opInst);
        
      } 
    } 
  }

  private void addOtherOpTypeInstances(SensornetOperator op, 
      HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {
    //data flows in parallel
    for (int k=0; k<op.getInDegree(); k++) 
    {
      //get child op
      SensornetOperator childOp = (SensornetOperator) op.getInput(k);
      //iterate over all instances of child operator
      Iterator<InstanceOperator> childOpInstIter =
        disconnectedOpInstMapping.get(childOp.getID()).iterator();
      while (childOpInstIter.hasNext()) 
      {
        InstanceOperator childOpInst = childOpInstIter.next();
        //get deepest site for this operator
        Site site = childOpInst.getDeepestConfluenceSite();
        //add operator to this site also.
        InstanceOperator opInst = new InstanceOperator(op,site);
        //update structures
        instanceDAF.addOpInst(op, opInst);
        instanceDAF.addEdge(childOpInst, opInst);
        
        disconnectedOpInstMapping.add(op.getID(), opInst);
      }
    }
  }
  
  private void addAttributeSensitiveOpInstances(SensornetOperator op,
      HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {//find deepest place to put operator. 
    Site dSite = findDeepestConfluenceSite(op, disconnectedOpInstMapping);  
    InstanceOperator opInst = new InstanceOperator(op, dSite);
    instanceDAF.addOpInst(op, opInst);
    disconnectedOpInstMapping.add(op.getID(), opInst);
    //sorts out edges eventually
    convergeAllChildOpSubstreams(op, opInst, instanceDAF, disconnectedOpInstMapping); 
  }



  private Site findDeepestConfluenceSite(SensornetOperator op,
      HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {//make new hash set
    HashSet<InstanceOperator> opInstSet = new HashSet<InstanceOperator>();
    //for each child of operator. add all instances of that child to hashset
    for (int i = 0; i<op.getInDegree(); i++) 
    {
      SensornetOperator childOp = (SensornetOperator) op.getInput(i);
      opInstSet.addAll(disconnectedOpInstMapping.get(childOp.getID()));
    }
    //iterate though routingtree from deep to shallow
    Iterator<Site> siteIter = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();      
      //locate instances which have a deepest confluence site as the current site
      HashSet<InstanceOperator> found = getConfluenceOpInstances(site, opInstSet);
      //if the instances coincide with set we're looking for, return the site
      if (found.equals(opInstSet)) 
      {
        return site;
      }
      
    }
    return null;
  }

  private HashSet<InstanceOperator> getConfluenceOpInstances(Site rootSite,
       HashSet<InstanceOperator> disconnectedChildOpInstances)
  {
    //create hashset to contain instance operators
    HashSet<InstanceOperator> InstanceOperators = new HashSet<InstanceOperator>();
    //Iterate over routing tree from deep to shallow
    Iterator<Site> siteIter = routingTree.siteIterator(rootSite, TraversalOrder.POST_ORDER);
    /*go though each site in the routing tree and check if any disconnected operators which 
     * have a deepest confluence site which is the site being checked. 
     */
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      
      Iterator<InstanceOperator> opInstIter = disconnectedChildOpInstances.iterator();
      while (opInstIter.hasNext()) 
      {
        InstanceOperator opInst = opInstIter.next();
        // if so then add to hashset
        if (opInst.getDeepestConfluenceSite()==site) 
        {
          InstanceOperators.add(opInst);
        }
      }
    }
    return InstanceOperators;
  }


  //aggr merge
  private void addIterativeOpInstances(SensornetOperator op, 
      HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {
    SensornetOperator childOp = (SensornetOperator) op.getInput(0);
    //convert to set so that we can do set equality operation later
    //holds all instances of the operators input
    HashSet<InstanceOperator> disconnectedChildOpInstSet = 
      new HashSet<InstanceOperator>(disconnectedOpInstMapping.get(childOp.getID()));
    //iterate over routing tree from bottom up
    Iterator<Site> siteIter = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      //gets instance operators which have a deepest confluence of the current site.
      HashSet<InstanceOperator> confluenceOpInstSet = 
        getConfluenceOpInstances(site, disconnectedChildOpInstSet);
      //if sets coincide (meaning all instances of input are located on site) break
      if (confluenceOpInstSet.equals(disconnectedChildOpInstSet)) 
      {
        break;
      }
      //if more than one instance on current site but not all
      if (confluenceOpInstSet.size()>1) 
      {
        //place new operator
        InstanceOperator opInst = new InstanceOperator(op, site);
        instanceDAF.addOpInst(op, opInst);
        //update children to new parent
        convergeSubstreams(confluenceOpInstSet, opInst, instanceDAF);
        //add new disconnected instance
        disconnectedChildOpInstSet.add(opInst);
        //remove what are now connected instances from the disconnected Operator hash
        disconnectedChildOpInstSet.removeAll(confluenceOpInstSet);
      }
    }
    //set all operators with this id to be the of input instances operators.
    disconnectedOpInstMapping.set(op.getID(), disconnectedChildOpInstSet);
    
    
  }

  private void addLocationSensitiveOpInstances(SensornetOperator op, 
               HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {
    int [] sourceSitesIDs;
    if(op instanceof SensornetAcquireOperator)//get source sites, as fixed locations
      sourceSitesIDs = op.getSourceSites();
    else
      sourceSitesIDs = new int [] {Integer.parseInt(routingTree.getRoot().getID())};

    //For each site, spawn an operator instance
    for (int sourceSiteIterator=0; sourceSiteIterator<sourceSitesIDs.length; sourceSiteIterator++) 
    {//get site object
      Site site = routingTree.getSite(sourceSitesIDs[sourceSiteIterator]);
      InstanceOperator opInst = new InstanceOperator(op, site);//make new instance of the operator
      instanceDAF.addOpInst(op, opInst);//add to instance dafs hashmap
      instanceDAF.assign(opInst, site);//put this operator on this site (placed)
      disconnectedOpInstMapping.add(op.getID(), opInst);//add to temp hash map which holds operators which dont have a connection upwards
      convergeAllChildOpSubstreams(op, opInst, instanceDAF, disconnectedOpInstMapping);
    }
  }

  /*
   * gets all inputs of children and gets the instance operators with the child name id
   */
  private void convergeAllChildOpSubstreams(SensornetOperator op,
                                           InstanceOperator opInst, 
                                           InstanceDAF instanceDAF2,
      HashMapList<String, InstanceOperator> disconnectedOpInstMapping)
  {
    for (int k=0; k<op.getInDegree(); k++) 
    {
      SensornetOperator childOp = (SensornetOperator) op.getInput(k);
      ArrayList<InstanceOperator> childOpInstColl 
        = disconnectedOpInstMapping.get(childOp.getID());
      convergeSubstreams(childOpInstColl, opInst, instanceDAF);
    }
  }

  //add an edge in the instance daf for each child operator
  private void convergeSubstreams(Collection<InstanceOperator> childOpInstColl,
                                  InstanceOperator opInst, InstanceDAF instanceDAF2)
  {
    Iterator<InstanceOperator> childOpInstIter = childOpInstColl.iterator();    
    while (childOpInstIter.hasNext()) 
    {
      InstanceOperator childOpInst = childOpInstIter.next();
      instanceDAF.addEdge(childOpInst, opInst);
    }
  }

  private void removeRedundantOpInstances() 
  throws OptimizationException
  {
    removeRedundantAggrIterOpInstances();
    removeRedundantSiblingOpInstances();  
  }
   
  private void removeRedundantSiblingOpInstances() 
  throws OptimizationException
  {
    //get iterator over instance operators
    Iterator<InstanceOperator> opInstIter = instanceDAF.iterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) {
      InstanceOperator opInst = opInstIter.next();
      HashMapList<Site, InstanceOperator> siteOpInstMap = new HashMapList<Site, InstanceOperator>();
      //for each child operator of op find its site and place into hashmap
      for (int i=0; i<opInst.getInDegree(); i++) {
        InstanceOperator childOpInst = (InstanceOperator)opInst.getInput(i);
        Site site = childOpInst.getSite();
        siteOpInstMap.add(site, childOpInst);
      }
      //go though hashmap pulling all instances on a site call instanceDAF merge sib
      Iterator<Site> siteIter = siteOpInstMap.keySet().iterator();
      while (siteIter.hasNext()) {
        Site site = siteIter.next();
        ArrayList<InstanceOperator> opInstColl = siteOpInstMap.get(site);
        instanceDAF.mergeSiblings(opInstColl);
      }
    }
    
  }

  private void removeRedundantAggrIterOpInstances() 
  throws OptimizationException
  {
    //iterate the operator instances
    Iterator<InstanceOperator> opInstIter = instanceDAF.iterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) 
    {
      InstanceOperator opInst = opInstIter.next();
      //if the operator is a agg merge or aggr eval
      if (   opInst.getInstanceOperator() instanceof SensornetAggrMergeOperator 
          || opInst.getInstanceOperator() instanceof SensornetAggrEvalOperator) 
      {
        /*check all children operators  for a agg merge which is on the same site as the op.
         * if so then remove child operator
         */
        for (int i=0; i<opInst.getInDegree(); i++) 
        {
          InstanceOperator childOpInst = (InstanceOperator)opInst.getInput(i);
          Site opSite = opInst.getSite();
          Site childOpSite = childOpInst.getSite();
          if (   childOpInst.getInstanceOperator() instanceof SensornetAggrMergeOperator 
              && opSite == childOpSite) 
          {
            instanceDAF.removeOpInst(childOpInst);
          }
        }
      }
    }
    
  }

  public void addAgenda(Agenda agenda)
  {
    this.agenda = agenda;
  }  
}