package uk.ac.manchester.cs.snee.compiler.iot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SNEEAlgebraicForm;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;


public class IOT extends SNEEAlgebraicForm
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 6858173999910587706L;
  private final static Logger logger = Logger.getLogger(IOT.class.getName());
  
  
  private HashMapList<Site,InstanceOperator> siteToOpInstMap 
      = new HashMapList<Site,InstanceOperator>();
  private HashMapList<String,InstanceOperator> opInstMapping 
      = new HashMapList<String,InstanceOperator>();
  private final HashSet<InstanceFragment> fragments 
      = new HashSet<InstanceFragment>();  
  private InstanceFragment rootFragment;
  private Tree instanceOperatorTree = new Tree();
  private InstanceOperator rootOp;
  private final RT rt;
  private PAF paf;
  private DAF cDAF;
  protected boolean showTupleTypes = false;
  protected static int candidateCount = 0;
  
  
  public IOT(final PAF paf, final RT rt, final String queryName) 
  throws SNEEException, SchemaMetadataException
  {
    super(queryName);
    this.paf = paf;
    this.rt=rt;
  }

  
  /**
   * creates a iterator for all fragments in IOT. 
   * @param traversalOrder which order iterator goes in
   * Post order or Pre order
   * @return
   */
  public final Iterator<InstanceFragment> instanceFragmentIterator(final 
      TraversalOrder traversalOrder) 
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER fragmentIterator()"); 
    final ArrayList<InstanceFragment> fragList = new ArrayList<InstanceFragment>();
    this.doFragmentIterator(this.rootFragment, fragList,
          traversalOrder);
    if (logger.isDebugEnabled())
     logger.debug("RETURN fragmentIterator()");
    return fragList.iterator();
   }

  /**
   * helper method for fragment iterator
   * @param frag
   * @param fragList
   * @param traversalOrder
   */
  private void doFragmentIterator(InstanceFragment frag,
      ArrayList<InstanceFragment> fragList, TraversalOrder traversalOrder)
  {
    if (logger.isTraceEnabled())
      logger.trace("ENTER doFragmentIterator()"); 
      if (traversalOrder == TraversalOrder.PRE_ORDER) {
          fragList.add(frag);
      }

      for (int n = 0; n < frag.getChildFragments().size(); n++) {

          this.doFragmentIterator(frag.getChildFragments().get(n), 
              fragList, traversalOrder);
      }

      if (traversalOrder == TraversalOrder.POST_ORDER) {
          fragList.add(frag);
      }
      if (logger.isTraceEnabled())
      logger.trace("RETURN doFragmentIterator()"); 
  }

  /**
   * set the root operator for the IOT
   * @param rootOp
   */
  public void setRoot(InstanceOperator rootOp)
  {
    this.rootOp = rootOp;
    instanceOperatorTree.setRoot(rootOp);
  }
  
  
  /**
   * returns all sites within the iot. (not just participating sites in rt.)
   * @return
   */
  public Iterator<Site> siteIterator()
  {
    return this.siteToOpInstMap.keySet().iterator();
  }
  
  
  /**
   * return the root operator for the IOT
   * @return
   */
  public InstanceOperator getRoot() 
  {
    return this.rootOp;
  }
  
  /**
   * adds a fragment to the fragment array.
   * @param frag
   */
  public void addInstanceFragment(InstanceFragment frag)
  {
    fragments.add(frag);
  }
  
  /**
   * gets the operators which are on a site (in the form of instance operators does not include exchanges)
   * @param site site to which the operators are being found on
   * @return the operators on site site in the form of a arraylist no order to the array
   */
  public ArrayList<InstanceOperator> getOpInstances(Site site) 
  {
    return this.siteToOpInstMap.get(site);
  }
  
  /**
   * gets the operators which are on a site (in the form of instance operators, and can include exchanges)
   * @param site site to which the operators are being found on
   * @param traversalOrder post or pre order of operators in tree.
   * @return the operators on site site in the form of a arraylist in a order 
   * defined in traversal order
   */
  public ArrayList<InstanceOperator> getOpInstances(Site site, TraversalOrder traversalOrder, 
                                     boolean exchanges) 
  {
    InstanceOperator root = this.rootOp; 
    final ArrayList<InstanceOperator> operatorList = new ArrayList<InstanceOperator>();
    this.doTransvesalIterator(root, site, operatorList, traversalOrder, exchanges);
    return operatorList;
  }
  
  /**
   * gets the operators which are on a site in an order(in the form of instance operators, and can include exchanges)
   * @param site site to which the operators are being found on
   * @return the operators on site site in the form of a arraylist in a order 
   * defined in traversal order
   */
  public ArrayList<InstanceOperator> getOpInstances(Site site, boolean exchanges) 
  {
    InstanceOperator root = this.rootOp; 
    final ArrayList<InstanceOperator> operatorList = new ArrayList<InstanceOperator>();
    this.doTransvesalIterator(root, site, operatorList, TraversalOrder.POST_ORDER, exchanges);
    return operatorList;
  }
  
  /**
   * helper method to produce order of operators
   * @param rootFragment2
   * @param operatorList
   * @param traversalOrder
   */
  private void doTransvesalIterator(InstanceOperator instanceOperator, Site site,
      ArrayList<InstanceOperator> operatorList, TraversalOrder traversalOrder,
      boolean exchanges)
  {
    // TODO Auto-generated method stub
    if (logger.isTraceEnabled())
      logger.trace("ENTER doFragmentIterator()"); 
      String currentSiteID = instanceOperator.getSite().getID();
      String lookingSiteID = site.getID();
      if (traversalOrder == TraversalOrder.PRE_ORDER && 
          currentSiteID.equals(lookingSiteID)) 
      {
        if(exchanges || !(instanceOperator instanceof InstanceExchangePart))
          operatorList.add(instanceOperator);
      }

      for (int n = 0; n < instanceOperator.getInDegree(); n++) 
      {
          this.doTransvesalIterator((InstanceOperator)instanceOperator.getInput(n), 
                                     site, operatorList, traversalOrder, exchanges);
      }

      if (traversalOrder == TraversalOrder.POST_ORDER && 
          currentSiteID.equals(lookingSiteID)) 
      {
        if(exchanges || !(instanceOperator instanceof InstanceExchangePart))
          operatorList.add(instanceOperator);
      }
      if (logger.isTraceEnabled())
      logger.trace("RETURN doTransversalIterator()"); 
  }


  /**
   * removes all operators from a site, and removes the site from the IOT
   * @param site the site to be removed
   * @throws OptimizationException 
   */
  public void removeSite(Site site) throws OptimizationException
  {
    ArrayList<InstanceOperator> operatorsOnSite = getOpInstances(site);
    Iterator<InstanceOperator> operatorIterator = operatorsOnSite.iterator();
    while(operatorIterator.hasNext())
    {
      InstanceOperator operator = operatorIterator.next();
      this.removeOpInst(operator);
    }
  }
  
  /**
   * adds a instance operators of a physical operator.
   * @param op the physical operator,
   * @param opInst the instance operator.
   */
  public void addOpInst(SensornetOperator op, InstanceOperator opInst) 
  {
    instanceOperatorTree.addNode(opInst);
    this.opInstMapping.add(op.getID(), opInst);
  }
  
  /**
   * add a operator instance to a site
   * @param opInst instance operator to add
   * @param site to add instance operator to.
   */
  public void addOpInstToSite(InstanceOperator opInst, Site site)
  {
    this.siteToOpInstMap.add(site, opInst);
  }
  
  /**
   * get all instances of a physical operator
   * @param op the physical operator
   * @return array list of the instance operators.
   */
  public ArrayList<InstanceOperator> getOpInstances(SensornetOperator op) 
  {
    if(op instanceof SensornetExchangeOperator)
    {
      ArrayList<InstanceOperator> exchangeOperators = new ArrayList<InstanceOperator>();
      Iterator<InstanceOperator> operatorIterator = this.treeIterator(TraversalOrder.POST_ORDER);
      while(operatorIterator.hasNext())
      {
        InstanceOperator operator = operatorIterator.next();
        if(operator instanceof InstanceExchangePart)
        {
          exchangeOperators.add(operator);
        }
      }
      return exchangeOperators;
    }
    return this.opInstMapping.get(op.getID()); 
  }
  
  /**
   * update the list of the instance operators for a physical operator.
   * @param op physical operator
   * @param opInstColl list of instance operators
   */
  public void setOpInstances(SensornetOperator op, Collection<InstanceOperator> opInstColl) 
  {
    this.opInstMapping.set(op.getID(), opInstColl);
  }
  
  /**
   * iterator over all instance operators within the iot
   * @return
   */
  public Iterator<InstanceOperator> iterateOverInstanceOperators()
  {
    Iterator<Node> phOps = paf.getOperatorTree().nodeIterator(TraversalOrder.POST_ORDER);
    ArrayList<InstanceOperator> ops = new  ArrayList<InstanceOperator>();
    while(phOps.hasNext())
    {
      Node op = phOps.next();
      ops.addAll(this.opInstMapping.get(op.getID()));
    }
    return ops.iterator();
  }
  
  /**
   * get number of instances of a physical operator
   * @param op the physical operator
   * @return the number of instances of the physical operator.
   */
  public int getNumOpInstances(SensornetOperator op) 
  {
    return this.getOpInstances(op).size();
  }
  
  /**
   * place a instance operator on a site
   * @param opInst the instance operator
   * @param site the site to place the instance op
   */
  public void assign(InstanceOperator opInst, Site site) 
  {  
    opInst.setSite(site);
    this.siteToOpInstMap.add(site, opInst);
  }
  
  /**
   * move a instance operator from a site to a new site.
   * @param opInst instance operator
   * @param newSite new loc
   * @param oldSite old loc
   */
  public void reAssign(InstanceOperator opInst, Site newSite, Site oldSite) 
  {
    opInst.setSite(newSite);
    this.siteToOpInstMap.remove(oldSite, opInst);
    this.siteToOpInstMap.add(newSite, opInst);
  }

  /**
   * get a set of all sites containing a physical operator
   * @param op the physical operator.
   * @return
   */
  public HashSet<String> getSites(SensornetOperator op) 
  {
    ArrayList<InstanceOperator> opInstances = this.getOpInstances(op); 
    HashSet<String> sites = new HashSet<String>();
    for (int i=0; i<opInstances.size(); i++) 
    {
      InstanceOperator opInst = opInstances.get(i);
      if(opInst.getSite() != null)
        sites.add(opInst.getSite().getID());
    }
    return sites;
  }
  
/**
 * tree iterator for all the instance operators
 * @param Order order the iterator goes in
 * @return a iterator
 */
  public Iterator<InstanceOperator> treeIterator(TraversalOrder Order)
  {
    final ArrayList<InstanceOperator> nodeList = 
      new ArrayList<InstanceOperator>();
    this.doIterator(this.getRoot(), nodeList, Order, true);

    return nodeList.iterator();
  }
  
  /**
   * tree iterator for all the instance operators includes exchanges if exchanges = true
   * @param order order the iterator goes in
   * @param exchanges
   * @return a iterator
   */
    public Iterator<InstanceOperator> treeIterator(TraversalOrder order, boolean exchanges)
    {
      final ArrayList<InstanceOperator> nodeList = 
        new ArrayList<InstanceOperator>();
      this.doIterator(this.getRoot(), nodeList, order, exchanges);

      return nodeList.iterator();
    }
  
  /**
   * iterator over a sub tree of the IOT
   * @param Order order the iterator goes in
   * @param node sub tree root operator
   * @return a iterator
   */
  public Iterator<InstanceOperator> subTreeIterator(TraversalOrder Order, InstanceOperator node)
  {
    final ArrayList<InstanceOperator> nodeList = 
      new ArrayList<InstanceOperator>();
    this.doIterator(node, nodeList, Order, true);

    return nodeList.iterator();
  } 

  /**
   * helper method for instance operator iterator
   * @param node
   * @param nodeList
   * @param order
   */
  private void doIterator(InstanceOperator node, 
      ArrayList<InstanceOperator> nodeList, TraversalOrder order, boolean exchanges)
  {
    if (order == TraversalOrder.PRE_ORDER) 
    {
      if(exchanges || (!exchanges && !(node instanceof InstanceExchangePart)))
        nodeList.add(node);
    }

    for (int n = 0; n < node.getInDegree(); n++) 
    {
        this.doIterator((InstanceOperator)node.getInput(n), nodeList, order, exchanges);
    }
  
    if (order == TraversalOrder.POST_ORDER) 
    {
      if(exchanges || (!exchanges && !(node instanceof InstanceExchangePart)))
        nodeList.add(node);
    }
  }

  /**
   * produces a iterator for sites
   * @param Order order the iterator goes in
   * @return a iterator
   */
  public Iterator<Site> siteIterator(TraversalOrder Order)
  {
    final ArrayList<Site> nodeList = 
      new ArrayList<Site>();
    this.doSiteIterator(this.getRoot().getSite(), nodeList, Order);

    return nodeList.iterator();
  }
  
  /**
   * produces a iterator for sites as a sub tree from root
   * @param Order order the iterator goes in
   * @param root the root site
   * @return a iterator
   */
  public Iterator<Site> subTreeSiteIterator(TraversalOrder Order, Site root)
  {
    final ArrayList<Site> nodeList = 
      new ArrayList<Site>();
    this.doSiteIterator(root, nodeList, Order);

    return nodeList.iterator();
  }
  
  
  /**
   * helper method for site iterator
   * @param root
   * @param nodeList
   * @param order
   */
  private void doSiteIterator(Site root, ArrayList<Site> nodeList,
      TraversalOrder order)
  {
    if (order == TraversalOrder.PRE_ORDER) 
    {
      nodeList.add(root);
    }

    for (int n = 0; n < root.getInDegree(); n++) 
    {
        Node input = root.getInput(n);
        logger.fatal(input.getID());
        this.doSiteIterator((Site)input, nodeList, order);
    }
  
    if (order == TraversalOrder.POST_ORDER) 
    {
        nodeList.add(root);
    }
    
  }


/**
 * remove a specific instance operator
 * @param childOpInst
 * @throws OptimizationException
 */
  public void removeOpInst(InstanceOperator childOpInst) 
  throws OptimizationException
  {
    //get inputs and outputs of children
    Node[] inputs = childOpInst.getInputs();
    Node[] outputs = childOpInst.getOutputs();
    //check for something which is not possible
    if (outputs.length != 1)
        throw new OptimizationException("Unable to remove node " + childOpInst.getID()
          + " as it does not have exactly one output");
    //Replace the inputs output with the operators output (skipping)
    for (int i=0; i<inputs.length; i++) {
        inputs[i].replaceOutput(childOpInst, outputs[0]);        
    }
    //replace the outputs input with the ops first child.
    outputs[0].replaceInput(childOpInst, inputs[0]);
    //update graph
    instanceOperatorTree.addEdge(inputs[0], outputs[0]);
    
    for (int i=1; i<inputs.length; i++) {
      outputs[0].addInput(inputs[i]);
      instanceOperatorTree.addEdge(inputs[i], outputs[0]);
    }
    //remove operator instance from both graph and instanceDAF data structure
    instanceOperatorTree.removeNode(childOpInst.getID());
    siteToOpInstMap.remove(childOpInst.getSite(), childOpInst);
    
  }

  /**
   * removes duplicate operators (siblings)
   * @param siblings
   * @throws OptimizationException
   */
  public void mergeSiblings(ArrayList<InstanceOperator> siblings) 
  throws OptimizationException
  {
    //empty check
    if (siblings.isEmpty()) 
    {
      return;
    }
    
    //siblings check
    //get parent operator from first operator
    InstanceOperator firstParent = (InstanceOperator)siblings.get(0).getOutput(0);
    //get site first operator is located on
    Site firstSite = siblings.get(0).getSite();
    //go though rest of operators
    for (int i=1; i<siblings.size(); i++) 
    {
      InstanceOperator currentParent = (InstanceOperator)siblings.get(i).getOutput(0);
      if (currentParent!=firstParent) 
      {
        throw new OptimizationException("Not all operator instances provided are siblings");
      }
      Site currentSite = siblings.get(i).getSite();
      if (currentSite!=firstSite) 
      {
        throw new OptimizationException("Not all operator instances provided are on the same site");
      }
    }
    
    //merge siblings into the first...
    InstanceOperator firstSibling = siblings.get(0);
    for (int i=1; i<siblings.size(); i++) 
    {
      InstanceOperator currentSibling =  siblings.get(i);
      if(!(currentSibling.getSensornetOperator() instanceof SensornetAcquireOperator))
      {
        for (int j=0; j<currentSibling.getInDegree(); j++) 
        {
          InstanceOperator siblingChild = (InstanceOperator)currentSibling.getInput(j); 
          firstSibling.addInput(siblingChild);
          siblingChild.addOutput(firstSibling);
          instanceOperatorTree.addEdge(siblingChild, firstSibling);
        }
        instanceOperatorTree.removeNode(currentSibling.getID());
        siteToOpInstMap.remove(currentSibling.getSite(), currentSibling); 
        firstParent.removeInput(currentSibling);
      }
    }
  }

  /**
   * return iterator for input sites for a site.
   * @param op
   * @param site
   * @param index
   * @return
   */
  public Iterator<Site> getInputOperatorInstanceSites(
      SensornetOperatorImpl op, Site site, int index)
  {
    ArrayList<Site> results = new ArrayList<Site>();
    final SensornetOperator childOp = (SensornetOperator) op.getInput(index);
    if (childOp instanceof SensornetExchangeOperator) {
        results = ((SensornetExchangeOperator) childOp).getSourceSites(site);
    } else {
        results.add(site);
    }
  
    return results.iterator();
  }

  /**
   * get the root operator of a site
   * @param site which to find root operator
   * @return the instance operator at the root of the site 
   * (either a instance deliver or a instance Exchange Part)
   */
  public InstanceOperator getRootOperatorOfSite(Site site)
  {
    ArrayList<InstanceOperator> list = this.getOpInstances(site);
    return list.get(0);
  }
  
  /**
   * returns a instance fragment with the corresponding frag id.
   * @param fragid the fragments id
   * @return the instance fragment
   */
  public InstanceFragment getInstanceFragment(String fragid)
  {
    Iterator<InstanceFragment> fragIterator = fragments.iterator();
    while(fragIterator.hasNext())
    {
      InstanceFragment frag = fragIterator.next();
      if(frag.getID().equals(fragid))
        return frag;
    }
    return null;
  }
  
  /**
   * returns as a hash set the instance fragments located on leaf nodes.
   * @return hash set containing instance fragments on leaf nodes.
   */
  public HashSet<InstanceFragment> getLeafInstanceFragments()
  {
    HashSet<InstanceFragment> output = new HashSet<InstanceFragment>();
    Iterator<InstanceFragment> fragIterator = fragments.iterator();
    while(fragIterator.hasNext())
    {
      InstanceFragment frag = fragIterator.next();
      if(frag.isLeaf())
        output.add(frag);
    }
    return output;
  }
  
  public boolean hasSiteGotInstFrag( Site site, InstanceFragment frag)
  {
    return frag.site.getID().equals(site.getID());
  }
  
  @Override
  protected String generateID(String queryName)
  {
    candidateCount++;
    return queryName + "-DAF-" + candidateCount;  
  }

  @Override
  public String getDescendantsString()
  {
    return this.getID()+"-"+this.getPAF().getDescendantsString();
  } 
  
  public EdgeImplementation addEdge(Node source, Node dest)
  {
    return instanceOperatorTree.addEdge(source, dest);
  }
  
  public void removeEdge(Node source, Node dest)
  {
    instanceOperatorTree.removeEdge(source, dest);
  } 
  
  public InstanceFragment getRootInstanceFragment()
  {
    return rootFragment;
  }
  
  public void setRootInstanceFragment(InstanceFragment rootFragment)
  {
    this.rootFragment = rootFragment;
  }
  
  public Node getNode(int siteID)
  {
    return rt.getSite(siteID);
  }
  
  public Node getNode(String siteID)
  {
    return rt.getSite(siteID);
  }
  
  public PAF getPAF()
  {
    return paf;
  }
  
  public RT getRT()
  {
    return rt;
  }

  public Tree getOperatorTree()
  {
    return instanceOperatorTree;
  }
  
  public HashSet<InstanceFragment> getInstanceFragments()
  {
    return fragments;
  }
  
  public void setPaf(PAF paf)
  {
    this.paf = paf;
  }


  public void setDAF(DAF cDAF)
  {
    this.cDAF = cDAF;
  }


  public DAF getDAF()
  {
    return cDAF;
  }


  /**
   * locates the edge corraspondign to the parent link
   * @param childOpInst
   * @return
   */
  public Edge getTransmissionEdge(InstanceOperator childOpInst)
  {
    HashSet<EdgeImplementation> edgesOfOp = getOperatorTree().getNodeEdges(childOpInst.getID());
    Iterator<EdgeImplementation> edgeIterator = edgesOfOp.iterator();
    while(edgeIterator.hasNext())
    {
      EdgeImplementation edge = edgeIterator.next();
      if(edge.getSourceID().equals(childOpInst.getID()))
        return edge;  
    }
    return null;
  }
  
  /**
   * gets all exchange operators located on a site
   * @param site
   * @return
   */
  public ArrayList<InstanceExchangePart> getExchangeOperators(Site site)
  {
    ArrayList<InstanceExchangePart> outwardsExchanges = new ArrayList<InstanceExchangePart>();
    ArrayList<InstanceOperator> instanceOperatorsOnSite = this.getOpInstances(site, true);
    Iterator<InstanceOperator> operatorsOnSiteIterator = instanceOperatorsOnSite.iterator();
    while(operatorsOnSiteIterator.hasNext())
    {
      InstanceOperator op = operatorsOnSiteIterator.next();
      if(op instanceof InstanceExchangePart)
      {
        outwardsExchanges.add((InstanceExchangePart) op);
      }
    }
    return outwardsExchanges;
  }

  /**
   * gets the instance operator corresponding to a certain id
   * @param operatorID
   * @return
   */
  public InstanceOperator getOperatorInstance(String operatorID)
  {
   TreeMap<String, Node> instanceOperators = this.getOperatorTree().getAllNodes();
   if(instanceOperators.containsKey(operatorID))
    return (InstanceOperator) instanceOperators.get(operatorID);

    return null;
  }


  /**
   * gets the instance operator located on site site, which is of the sensornet operator op.
   * @param op
   * @param site
   * @return
   */
  public InstanceOperator getOperatorInstance(SensornetOperator op, Site site)
  {
    ArrayList<InstanceOperator> instanceOps = this.getOpInstances(op);
    Iterator<InstanceOperator> instanceOperatorIterator = instanceOps.iterator();
    while(instanceOperatorIterator.hasNext())
    {
      InstanceOperator instanceOp = instanceOperatorIterator.next();
      if(instanceOp.getSite().getID().equals(site.getID()))
        return instanceOp;
    }
    return null;
  }


  public ArrayList<Node> getInputSites(Site failedSite)
  {
    Node failedNode = this.rt.getSite(failedSite.getID());
    ArrayList<Node> inputSites = new ArrayList<Node>(failedNode.getInputsList());
    return inputSites;
  }
}