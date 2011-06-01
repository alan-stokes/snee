package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SNEEAlgebraicForm;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;


public class IOT extends SNEEAlgebraicForm
{
  private HashMapList<Site,InstanceOperator> siteToOpInstMap 
      = new HashMapList<Site,InstanceOperator>();
  private HashMapList<String,InstanceOperator> opInstMapping 
      = new HashMapList<String,InstanceOperator>();
  private final HashSet<InstanceFragment> fragments 
      = new HashSet<InstanceFragment>();  
  private InstanceFragment rootFragment;
  private Tree instanceOperatorTree = new Tree();
  private InstanceOperator rootOp;
  private RT rt;
  private PAF paf;
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
  public final Iterator<InstanceFragment> fragmentIterator(final 
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
  public void addFragment(InstanceFragment frag)
  {
    fragments.add(frag);
  }
  
  /**
   * gets the operators which are on a site (in the form of instance operators
   * @param site site to which the operators are being found on
   * @return the operators on site site in the form of a arraylist
   */
  public ArrayList<InstanceOperator> getOpInstances(Site site) 
  {
    return this.siteToOpInstMap.get(site);
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
  public HashSet<Site> getSites(SensornetOperator op) 
  {
    ArrayList<InstanceOperator> opInstances = this.getOpInstances(op); 
    HashSet<Site> sites = new HashSet<Site>();
    for (int i=0; i<opInstances.size(); i++) 
    {
      InstanceOperator opInst = opInstances.get(i);
      sites.add(opInst.getSite());
    }
    return sites;
  }
  
  /**
   * produce a image of the IOT as a dot file
   * @param fname name of file
   * @param label label for within the dot file.
   * @throws SchemaMetadataException
   */
  public void exportAsDOTFile(final String fname, final String label) 
  throws SchemaMetadataException
  {
    try
    {   
      PrintWriter out = new PrintWriter(new BufferedWriter(
            new FileWriter(fname)));
        out.println("digraph \"" + (String) instanceOperatorTree.getName() + "\" {");
        String edgeSymbol = "->";
        
        out.println("label = \"" + label + "\";");
        out.println("rankdir=\"BT\";");
        
         /**
         * Draw the operators on the site
         */
        final Iterator<Site> siteIter = this.rt.siteIterator(TraversalOrder.POST_ORDER);
        while (siteIter.hasNext()) 
        {
          final Site site = siteIter.next();
          ArrayList<InstanceOperator> opInstSubTree = this.getOpInstances(site);
          if (!opInstSubTree.isEmpty()) 
          {
            out.println("subgraph cluster_" + site.getID() + " {");
            out.println("style=\"rounded,dotted\"");
            out.println("color=blue;");
        
            final Iterator<InstanceOperator> opInstIter 
              = opInstSubTree.iterator();
            while (opInstIter.hasNext()) 
            {
                final InstanceOperator opInst = opInstIter.next();
                out.println("\"" + opInst.getID() + "\" ;");
            }
        
            out.println("fontsize=9;");
            out.println("fontcolor=red;");
            out.println("labelloc=t;");
            out.println("labeljust=r;");
            out.println("label =\"Site " + site.getID()+"\"");
            out.println("}\n");
          }
        }
        
        //traverse the edges now
        Iterator<String> i = instanceOperatorTree.getEdges().keySet().iterator();
        while (i.hasNext()) 
        {
          Edge e = instanceOperatorTree.getEdges().get((String) i.next());
          out.println("\"" + instanceOperatorTree.getAllNodes().get(e.getSourceID()).getID()
          + "\"" + edgeSymbol + "\""
          + instanceOperatorTree.getAllNodes().get(e.getDestID()).getID() + "\" ");
        }
        out.println("}");
        out.close();

    } 
    catch (IOException e) 
    {
      System.out.println("Export failed: " + e.toString());
        System.err.println("Export failed: " + e.toString());
    }
  }
  
  /**
   * exports the IOT as a dot file, but contains fragments, and allows to turn on or off exchange operators
   * @param fname  name of file
   * @param label label for within dot file
   * @param exchangesOnSites if exchange operators are broken down
   */
  public void exportAsDotFileWithFrags(final String fname, final String label, boolean exchangesOnSites)
  {
	  try
	  {
	    //create writer
	    PrintWriter out = new PrintWriter(new BufferedWriter( new FileWriter(fname)));
	    //create blurb
	    out.println("digraph \"" + (String) instanceOperatorTree.getName() + "\" {");
      out.println("label = \"" + label + "\";");
      out.println("rankdir=\"BT\";");
      out.println("compound=\"true\";");
      //itterate though sites in routing tree (depth to shallow)
      Iterator<Site> siteIterator = rt.siteIterator(TraversalOrder.POST_ORDER);
      while(siteIterator.hasNext())
      {//for each site do blurb and then go though fragments
    	  Site currentSite = siteIterator.next();
    	  out.println("subgraph cluster_s" + currentSite.getID() + " {");
    	  out.println("style=\"rounded,dotted\"");
    	  out.println("color=blue;");
    	  //get fragments within site
    	  Iterator<InstanceFragment> fragmentIterator = fragments.iterator();
    	  while(fragmentIterator.hasNext())
    	  {
    		  InstanceFragment currentFrag = fragmentIterator.next();
    		  if(currentFrag.getSite().getID().equals(currentSite.getID()))
    		  { //produce blurb
      		  out.println("subgraph cluster_f" + currentFrag.getID() + " {");
      		  out.println("style=\"rounded,dashed\"");
      	    out.println("color=red;");
      	    //go though operators printing ids
      	    Iterator<InstanceOperator> operatorIterator = currentFrag.operatorIterator(TraversalOrder.POST_ORDER);
      	    while(operatorIterator.hasNext())
      	    {
      	      InstanceOperator currentOperator = operatorIterator.next();
      	      out.println("\"" + currentOperator.getID() + "\" ;");
      	    }
      	    //output bottom blurb of a cluster
      	    out.println("fontsize=9;");
      	    out.println("fontcolor=red");
      	    out.println("labelloc=t;");
      	    out.println("labeljust=r;");
      	    out.println("label =\"frag " + currentFrag.getID() + "\"");
      	    out.println("}"); 
      		}
      	}
      	//add exchanges if needed
      	if(exchangesOnSites)
      	{
      	  HashSet<InstanceExchangePart> exchangeParts = currentSite.getInstanceExchangeComponents();
      	  Iterator<InstanceExchangePart> exchangePartsIterator = exchangeParts.iterator();
      	  while(exchangePartsIterator.hasNext())
      	  {
      		  InstanceExchangePart exchangePart = exchangePartsIterator.next();
      	    out.println("\"" + exchangePart.getID() + "\" ;");
      	  }
      	}
    	
      	//output bottom blurb of a site
      	out.println("fontsize=9;");
      	out.println("fontcolor=red");
      	out.println("labelloc=t;");
      	out.println("labeljust=r;");
      	out.println("label =\"site " + currentSite.getID() + "\"");
      	out.println("}");  
      }
      //get operator edges
      String edgeSymbol = "->";
      Iterator<String> i = instanceOperatorTree.getEdges().keySet().iterator();
      while (i.hasNext()) 
      {
        Edge e = instanceOperatorTree.getEdges().get((String) i.next());
        out.println("\"" + instanceOperatorTree.getAllNodes().get(e.getSourceID()).getID()
        + "\"" + edgeSymbol + "\""
        + instanceOperatorTree.getAllNodes().get(e.getDestID()).getID() + "\" ");
      }
      out.println("}");
      out.close();
    }
	  catch(IOException e)
	  {
	    System.out.println("Export failed: " + e.toString());
	    System.err.println("Export failed: " + e.toString());
	  }
  }
  
/**
 * iterator for the instance operators
 * @param Order order the iterator goes in
 * @return a iterator
 */
  public Iterator<InstanceOperator> iterator(TraversalOrder Order)
  {
    final ArrayList<InstanceOperator> nodeList = 
      new ArrayList<InstanceOperator>();
    this.doIterator(this.getRoot(), nodeList, Order);

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
    this.doIterator(node, nodeList, Order);

    return nodeList.iterator();
  } 

  /**
   * helper method for instance operator iterator
   * @param node
   * @param nodeList
   * @param order
   */
  private void doIterator(InstanceOperator node, 
      ArrayList<InstanceOperator> nodeList, TraversalOrder order)
  {
    if (order == TraversalOrder.PRE_ORDER) 
    {
      nodeList.add(node);
    }

    for (int n = 0; n < node.getInDegree(); n++) 
    {
        this.doIterator((InstanceOperator)node.getInput(n), nodeList, order);
    }
  
    if (order == TraversalOrder.POST_ORDER) 
    {
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
  
  @Override
  protected String generateID(String queryName)
  {
    candidateCount++;
    return queryName + "-DAF-" + candidateCount;  
  }

  @Override
  public String getDescendantsString()
  {
    return this.getID()+"-"+this.paf.getDescendantsString();
  } 
  
  public EdgeImplementation addEdge(Node source, Node dest)
  {
    return instanceOperatorTree.addEdge(source, dest);
  }
  
  public void removeEdge(Node source, Node dest)
  {
    instanceOperatorTree.removeEdge(source, dest);
  } 
  
  public InstanceFragment getRootFragment()
  {
    return rootFragment;
  }
  
  public void setRootFragment(InstanceFragment rootFragment)
  {
    this.rootFragment = rootFragment;
  }
  
  public Node getNode(int siteID)
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

}