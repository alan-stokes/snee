package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class InstanceWhereSchedular
{
  private RT routingTree;
  private IOT instanceDAF;
  private DAF cDAF;
  private CostParameters costs;
  private PAF paf;
  
  
  public InstanceWhereSchedular(PAF paf, RT routingTree, CostParameters costs) 
  throws SNEEException, SchemaMetadataException, OptimizationException, SNEEConfigurationException
  {
    this.paf = paf;
    this.routingTree = routingTree;
    this.costs = costs;
    buildInstanceDAF();
  }
  
  public void buildInstanceDAF() 
  throws SNEEException, SchemaMetadataException, OptimizationException, SNEEConfigurationException
  {
    //make directory withoutput folder to place cost model images
    String fileDirectory = SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) + "/costModelImages";
    //String fileDirectory = SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) + "/costModelImages";
    boolean success = new File(fileDirectory).mkdir();
    if(success)
    {
      //generate floating operators / fixed locations
      generatePartialDaf();
      //produce image output so that can be validated
      instanceDAF.exportAsDOTFile(fileDirectory + "/partialInstanceDAF.dot", "");
      //do heuristic placement
      doInstanceOperatorSiteAssignment();
      instanceDAF.exportAsDOTFile(fileDirectory + "/siteAssignment.dot", "");
      //move duplicate aggerates upwards
      moveAggeratesInitsUpwards();
      instanceDAF.exportAsDOTFile(fileDirectory + "/movedInitsUpwards.dot", "");
      //remove instances which are redundant
      removeRedundantOpInstances();
      instanceDAF.exportAsDOTFile(fileDirectory + "/cleanedSiteAssignment.dot", "");
      startFragmentation();
      instanceDAF.exportAsDotFileWithFrags(fileDirectory + "/fragmentedInstanceDAF.dot", "", false);
      addExchangeParts();
      createCDAF();
      updateOperatorLinksToIncludeExchangeParts();
      instanceDAF.exportAsDotFileWithFrags(fileDirectory + "/InstanceDAFWithExchangeLinksBuiltIN.dot", "", true);
    }    
    else
    {
      System.out.println("directory not makable");
    }
  }
  
  //XXX works, but doesnt link the exchanges to sites
  private void updateOperatorLinksToIncludeExchangeParts() 
  {
    Iterator<InstanceFragment> fragmentIterator = instanceDAF.fragmentIterator(TraversalOrder.POST_ORDER);
    //go though fragments
    while(fragmentIterator.hasNext())
    {
	  InstanceFragment currentFrag = fragmentIterator.next();//if frag on site
	  //get root operator
	  InstanceOperator rootOp = currentFrag.rootOperator;
	  if(!(rootOp.getInstanceOperator() instanceof SensornetDeliverOperator))
	  {
	    InstanceExchangePart exchange = currentFrag.getParentExchangeOperator();
	    exchange.addInput(rootOp);
	    //always have 1 output
	    rootOp.replaceOutput(rootOp.getOutput(0), exchange);
	    instanceDAF.addEdge(rootOp, exchange);
	    while(exchange.getNext() != null)
	      {
          InstanceExchangePart nextExchange = exchange.getNext(); 
          instanceDAF.addEdge(exchange, nextExchange);
	        exchange = nextExchange;  
        }
        //get root operator of higher frag frag
        InstanceOperator lowestOperator = exchange.getDestFrag().getLowestOperator();
        lowestOperator.replaceInput(rootOp, exchange);   
        instanceDAF.addEdge(exchange, lowestOperator);
        instanceDAF.removeEdge(rootOp, lowestOperator);
	  }
	}
  }

  //tested and works 
  private void moveAggeratesInitsUpwards()//aggerate inits
  {
    //go though routing tree from top to bottom
    Iterator<Site> siteIter = routingTree.siteIterator(TraversalOrder.PRE_ORDER);
    while(siteIter.hasNext())
    {
      Site currentSite = siteIter.next();//get site
      //get operators on site 
      ArrayList<InstanceOperator> currentSitesOperators = instanceDAF.getOpInstances(currentSite);
      Iterator<InstanceOperator> currentSiteOperatorIterator = currentSitesOperators.iterator();
      boolean found = false;
      InstanceOperator mergeInstance = null;
      //look though operators looking for a merge
      while(currentSiteOperatorIterator.hasNext() && !found)
      {
        InstanceOperator instance = currentSiteOperatorIterator.next();
        if(instance.getInstanceOperator() instanceof SensornetAggrMergeOperator)
        {
          found = true;
          mergeInstance = instance;
        }
      }
      //found an merge, look at children seeing if on same site, if not move to
      if(found)
      {
        
        for(int childrenOperatorIndex = 0; childrenOperatorIndex< mergeInstance.getInDegree(); childrenOperatorIndex++ )
        {
          //get child op
          InstanceOperator child = (InstanceOperator) mergeInstance.getInput(childrenOperatorIndex);
          if(child.getInstanceOperator() instanceof SensornetAggrInitOperator)
          {
            if(!child.getSite().getID().equals(mergeInstance.getSite().getID()))
            {
              instanceDAF.reAssign(child, mergeInstance.getSite(), child.getSite());
            }
          }
        }
      }
    }
  }

  private void startFragmentation()
  {
    Iterator<InstanceOperator> InstanceOperatorIterator = instanceDAF.iterator(TraversalOrder.PRE_ORDER);
    InstanceFragment fragment = new InstanceFragment();
    fragmentate(InstanceOperatorIterator, fragment);
  }
  

  private Iterator<InstanceOperator> fragmentate(
                                        Iterator<InstanceOperator> InstanceOperatorIterator
                                      , InstanceFragment currentFragment)
  {
    if(InstanceOperatorIterator.hasNext())
    {
      InstanceOperator instance = InstanceOperatorIterator.next();
      InstanceOperator inputOp;
      if(!(instance.getInstanceOperator() instanceof SensornetAcquireOperator))
        inputOp = (InstanceOperator) instance.getInput(0);
      else
        inputOp = instance;
      
      if(    instance.getInDegree() > 1 
          || instance.getInstanceOperator() instanceof SensornetAcquireOperator
          || !instance.isRemote(inputOp))
      {
        currentFragment.addOperator(instance);
        checkRoot(currentFragment, instance);
          
        for(int input = 0; input < instance.getInDegree(); input++ )
        {
          InstanceFragment newFrag = new InstanceFragment();
          newFrag.setNextHigherFragment(currentFragment);
          currentFragment.addNextLowerFragment(newFrag);
          InstanceOperatorIterator = fragmentate(InstanceOperatorIterator, newFrag);
        }
        instanceDAF.addFragment(currentFragment);
        if(currentFragment.getRootOperator().getInstanceOperator()
           instanceof SensornetDeliverOperator)
        {
          instanceDAF.setRootFragment(currentFragment);
        }
      }
      else
      {
        currentFragment.addOperator(instance);
        checkRoot(currentFragment, instance);
        fragmentate(InstanceOperatorIterator, currentFragment);
      }
    }
    return InstanceOperatorIterator;
  }
  
  private void checkRoot(InstanceFragment currentFragment, InstanceOperator instance )
  {
    if(currentFragment.getRootOperator() == null)
    {
      currentFragment.setRootOperator(instance);
      currentFragment.setSite(instance.getSite());
    }
  }
  
  private void addExchangeParts()
  {
    //itterate though fragments in post order
    Iterator<InstanceFragment> InstanceFragmentIterator 
    = instanceDAF.fragmentIterator(TraversalOrder.POST_ORDER);
    //get each fragment and link to parent fragment via exchanges
    while(InstanceFragmentIterator.hasNext())
    {
      InstanceFragment instance = InstanceFragmentIterator.next();  
      InstanceFragment parent = instance.getNextHigherFragment();
      
      if(!(instance.getRootOperator().getInstanceOperator() instanceof SensornetDeliverOperator))
      {       
        if (instance.isRemote(parent))//may be many jumps and require relays
        {
          Path routeBetweenNodes = routingTree.getPath(instance.getSite().getID(), 
                                                       parent.getSite().getID());
          Iterator<Site> routeIterator = routeBetweenNodes.iterator();
          
          InstanceExchangePart lastPart = null;
          while(routeIterator.hasNext())
          {
            Site currentPathSite = routeIterator.next();
            InstanceExchangePart part = null;
            if(currentPathSite == instance.getSite())
            {
              part = new InstanceExchangePart(instance, instance.site, parent, parent.site, 
                                              currentPathSite, ExchangePartType.PRODUCER, false, 
                                              null);
              lastPart = part;
              instance.setParentExchange(part);
            }
            else
            {
              if(currentPathSite == parent.getSite())
              {
                part = new InstanceExchangePart(instance, instance.site, parent, parent.site, 
                                                currentPathSite, ExchangePartType.CONSUMER, false, 
                                                lastPart);
                lastPart = part;
                parent.addChildExchange(part);
                
              }
              else
              {
                part = new InstanceExchangePart(instance, instance.site, parent, parent.site, 
                                                currentPathSite, ExchangePartType.RELAY, false, 
                                                lastPart);
                lastPart = part;
              }
            }
          }
        }
        else//on same site, just add a consumer and producer to sort out fragment.
        {
          //produce a consumer and producer for the link, both on the same site
          InstanceExchangePart part 
          = new InstanceExchangePart(instance, instance.site, parent, parent.site, 
                                     parent.site, ExchangePartType.PRODUCER, false, 
                                     null);
          InstanceExchangePart part2 
          = new InstanceExchangePart(instance, instance.site, parent, parent.site, 
                                     parent.site, ExchangePartType.CONSUMER, false, 
                                     part);
          instance.setParentExchange(part);
          parent.addChildExchange(part2);
        }
      }
    }
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
          assignRecursiveOpInstances(instance);
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

  private void assignRecursiveOpInstances(InstanceOperator instance) //agg merge
  {
    boolean assigned = false;
    ArrayList<InstanceOperator> childOperatorInstances = new ArrayList<InstanceOperator>();
    //build list of child instances
    for(int i = 0; i < instance.getInDegree(); i++)
    {
      InstanceOperator childOp =(InstanceOperator)instance.getInput(i);
      childOperatorInstances.add(childOp);     
    }
    
    Iterator<Site> siteIter = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIter.hasNext())
    {
      Site currentSite = siteIter.next();
      ArrayList<InstanceOperator> currentSiteOperatorInstances = instanceDAF.getOpInstances(currentSite);
     
      if(!currentSite.isLeaf() && !assigned && !currentSiteOperatorInstances.contains(instance))
      {
        /**
         * 1. not a leaf node, so no basic acquires, not assigned already, and currentsite doesnt hold a instance of the operator
         * 2. iterate over inputs, checking if child operator is either an child or current
         * 3. count the times it satisfies the search (2 = place instance here)
         */
        int satisifiedSearch = 0;
        if(checkArray(currentSiteOperatorInstances, childOperatorInstances))
          satisifiedSearch++;
        
        for(int currentInputIndex = 0; currentInputIndex < currentSite.getInDegree();
            currentInputIndex++)
        {
          Site inputSite = (Site) currentSite.getInput(currentInputIndex);
          //check if instance of current or child
          ArrayList<InstanceOperator> operatorsOnSite = instanceDAF.getOpInstances(inputSite);
          while(operatorsOnSite.size() == 0)
          {
            inputSite = (Site) inputSite.getInput(0);
            operatorsOnSite = instanceDAF.getOpInstances(inputSite);
          }
          
          if(checkArray(operatorsOnSite, childOperatorInstances))
            satisifiedSearch++;
        }
        if(satisifiedSearch >= 2)//found two instances which fit criteria
        {
          //assign instance to this site
          instanceDAF.assign(instance, currentSite); 
          assigned = true;
        }
      }
    }
    if(!assigned)
    {
      System.out.println("instance " + instance.getID() + " cant be placed for some reason");
    }
  }
  
  private boolean checkArray(ArrayList<InstanceOperator> operatorInstances,
      ArrayList<InstanceOperator> childOperatorInstances)
  {
    for(int childInstanceIndex = 0; childInstanceIndex < childOperatorInstances.size(); childInstanceIndex++)
      if(operatorInstances.contains(childOperatorInstances.get(childInstanceIndex)))
        return true;
    return false;
  }

  //tested and works
  private void assignAttributeSensitiveOpInstances(InstanceOperator instance)
  {
    boolean assigned = false;
    ArrayList<InstanceOperator> operatorInstances = new ArrayList<InstanceOperator>();
    //build list of child instances
    for(int i = 0; i < instance.getInDegree(); i++)
    {
      InstanceOperator childOp =(InstanceOperator)instance.getInput(i);
      operatorInstances.add(childOp);
    }//locate the deepest site which has all instances below it, and then assign it to there
    Iterator<Site> siteIter = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIter.hasNext())
    {
      Site currentSite = siteIter.next();
      ArrayList<InstanceOperator> currentSiteOperatorInstances = instanceDAF.getOpInstances(currentSite);
      int satisifiedSearch = 0;
      if(!currentSite.isLeaf() && !assigned && !currentSiteOperatorInstances.contains(instance))
      {
        /**
         * 1. not a leaf node, so no basic acquires, not assigned already, and currentsite doesnt hold a instance of the operator
         * 2. iterate over inputs, checking if child operator is either an child or current
         * 3. count the times it satisfies the search (2 = place instance here)
         */
        if(checkArray(currentSiteOperatorInstances, operatorInstances))
          satisifiedSearch++;
        
        for(int currentInputIndex = 0; currentInputIndex < currentSite.getInDegree();
            currentInputIndex++)
        {
          Site inputSite = (Site) currentSite.getInput(currentInputIndex);
          //check if instance of current or child
          ArrayList<InstanceOperator> operatorsOnSite = instanceDAF.getOpInstances(inputSite);
          
          if(checkArray(operatorsOnSite, operatorInstances))
            satisifiedSearch++;
          
          satisifiedSearch = checkSubTree(inputSite, satisifiedSearch, operatorInstances);

        }
      }
        if(satisifiedSearch == operatorInstances.size())//found all instances which fit criteria
        {
          //assign instance to this site
          instanceDAF.assign(instance, currentSite); 
          assigned = true;
        }
      }
    if(!assigned)
    {
      System.out.println("instance " + instance.getID() + " cant be placed for some reason");
    }
  }
  
  private int checkSubTree(Site input, int satisifiedSearch, ArrayList<InstanceOperator> operatorInstances)
  {
    for(int inputSiteIndex = 0; inputSiteIndex <  input.getInDegree(); inputSiteIndex ++)
    {
       Site newInput = (Site) input.getInput(inputSiteIndex);
       ArrayList<InstanceOperator> operatorsOnSite = instanceDAF.getOpInstances(newInput);
       if(checkArray(operatorsOnSite, operatorInstances))
           satisifiedSearch++;
       satisifiedSearch = checkSubTree(newInput, satisifiedSearch, operatorInstances);  
    }
    return satisifiedSearch;
  }
  
  private void generatePartialDaf() 
  throws SNEEException, SchemaMetadataException
  {
    //make new instance daf
    instanceDAF = new IOT(paf, routingTree, paf.getQueryName());
    HashMapList<String,InstanceOperator> disconnectedOpInstMapping =
      new HashMapList<String,InstanceOperator>();
    //collect a iterator for physical operators 
    Iterator<SensornetOperator> opIter = paf.operatorIterator(TraversalOrder.POST_ORDER);
    /*iterate though physical operators looking at each and determining 
    how many instances there should be */
    while (opIter.hasNext())
    {
      SensornetOperator op = opIter.next();
      
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
      HashSet<InstanceOperator> found = getConfluenceOpInstances(site, opInstSet, false);
      //if the instances coincide with set we're looking for, return the site
      if (found.equals(opInstSet)) 
      {
        return site;
      }
      
    }
    return null;
  }

  private HashSet<InstanceOperator> getConfluenceOpInstances(Site rootSite,
       HashSet<InstanceOperator> disconnectedChildOpInstances, boolean assign)
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
        if(assign)
        {
       // if so then add to hashset
          if (opInst.getSite()==site) 
          {
            InstanceOperators.add(opInst);
          }
        }
        else
        {
          // if so then add to hashset
          if (opInst.getDeepestConfluenceSite()==site) 
          {
            InstanceOperators.add(opInst);
          }
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
        getConfluenceOpInstances(site, disconnectedChildOpInstSet, false);
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
                                           IOT instanceDAF2,
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
                                  InstanceOperator opInst, IOT instanceDAF2)
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
    removeRedundantAggrIterOpAfterInitMergeInstances();
  }
   
  private void removeRedundantAggrIterOpAfterInitMergeInstances() 
  throws OptimizationException
  {
    Iterator<InstanceOperator> opInstIter = instanceDAF.iterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) 
    {
      InstanceOperator operator = opInstIter.next();
      if(operator.getInstanceOperator() instanceof SensornetAggrMergeOperator)
      {
        if(operator.getInDegree() == 1)
        {
          instanceDAF.removeOpInst(operator);
        }
      }
    }
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
  
  private void createCDAF() 
  throws OptimizationException, SNEEException, SchemaMetadataException
  {
    DAF faf = partitionPAF(paf, instanceDAF, routingTree.getQueryName(), routingTree, costs);
    //faf.display(SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR), faf.getName(), "faf");
    
    cDAF = linkFragments(faf, routingTree, instanceDAF, routingTree.getQueryName());
    //cDAF.display(SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR),
    //    cDAF.getQueryName(),
    //    "cdaf");
    
    removeRedundantAggrIterOp(cDAF);
    
  }
  
  private static DAF partitionPAF(final PAF paf, IOT oit, 
                                  final String queryName, RT routingTree,
                                  CostParameters costs) 
  throws SNEEException, SchemaMetadataException 
  {
    DAF faf = new DAF(paf, routingTree, queryName);
    
    //Get rid of unecessary aggrIter in FAF... (i.e., they have not been assigned to any site)
    Iterator<SensornetOperator> opIter = faf
    .operatorIterator(TraversalOrder.POST_ORDER);
    while (opIter.hasNext()) {
      final SensornetOperator op = (SensornetOperator) opIter.next();
      HashSet<Site> opSites = oit.getSites(op);
      if (opSites.size()==0) {
        try {
          faf.getOperatorTree().removeNode(op);
        } catch (OptimizationException e) {
          e.printStackTrace();
        }
      }
    }
    //Insert exchanges where necessary to partition the query plan
    opIter = faf.operatorIterator(TraversalOrder.POST_ORDER);
    while (opIter.hasNext()) {
      final SensornetOperator op = (SensornetOperator) opIter.next();
      HashSet<Site> opSites = oit.getSites(op);     
      
      if (op instanceof SensornetAggrMergeOperator) {
        final SensornetOperator childOp = (SensornetOperator) op.getInput(0);
          final SensornetExchangeOperator exchOp = new SensornetExchangeOperator(costs);
            faf.getOperatorTree().insertNode(childOp, op, exchOp);        
      } else {
        for (int i=0; i<op.getInDegree(); i++) {
          final SensornetOperator childOp = (SensornetOperator) op.getInput(i);
          
          HashSet<Site> childSites = oit.getSites(childOp);
          if (!opSites.equals(childSites)) {
            final SensornetExchangeOperator exchOp = new SensornetExchangeOperator(costs);   
            faf.getOperatorTree().insertNode(childOp, op, exchOp);
          }
        }
      }
    }

    faf.buildFragmentTree();
    return faf;
  }
  
  private static DAF linkFragments(DAF faf, RT rt, IOT daf,
      String queryName) throws SNEEException, SchemaMetadataException {
    DAF cDAF = faf;
    
    Iterator<InstanceOperator> opInstIter = daf.iterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) {
      InstanceOperator opInst = opInstIter.next();
      //have to get the cloned copy in compactDaf...
      SensornetOperator op = (SensornetOperator)cDAF.getOperatorTree().getNode(opInst.getInstanceOperator().getID());

      Site sourceSite = (Site)cDAF.getRT().getSite(opInst.getSite().getID());
      Fragment sourceFrag = op.getContainingFragment();
      
      if (op.getOutDegree() > 0) {
        SensornetOperator parentOp = op.getParent();

        if (parentOp instanceof SensornetExchangeOperator) {

          InstanceOperator paOpInst = (InstanceOperator)opInst.getOutput(0);
          Site destSite = (Site)cDAF.getRT().getSite(paOpInst.getSite().getID());
          Fragment destFrag = ((SensornetOperator)cDAF.getOperatorTree().getNode(((InstanceOperator)opInst.getOutput(0)).getInstanceOperator().getID())).getContainingFragment();
          final Path path = cDAF.getRT().getPath(sourceSite.getID(), destSite.getID());
          
          cDAF.placeFragment(sourceFrag, sourceSite);
          cDAF.linkFragments(sourceFrag, sourceSite, destFrag, destSite, path);
        }       
      } else {  
        cDAF.placeFragment(sourceFrag, sourceSite);
      }
    }
    return cDAF;
  }


  private static void removeRedundantAggrIterOp(DAF daf) throws OptimizationException {

    Iterator<SensornetOperator> opIter = daf.operatorIterator(TraversalOrder.POST_ORDER);
    while (opIter.hasNext()) {
      SensornetOperator op = opIter.next();
      if (op instanceof SensornetAggrMergeOperator) {
        if (!(op.getParent() instanceof SensornetExchangeOperator)) {
          daf.getOperatorTree().removeNode(op);
        }
      }
    }
  }
  
  public DAF getDAF()
  {
    return cDAF;
  }  
  
  public IOT getInstanceDAF()
  {
    return instanceDAF;
  }
}