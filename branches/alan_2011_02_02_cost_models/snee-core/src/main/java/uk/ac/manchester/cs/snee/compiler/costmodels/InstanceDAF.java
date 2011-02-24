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


public class InstanceDAF extends SNEEAlgebraicForm
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
  
  
  public InstanceDAF(final PAF paf, final RT rt, final String queryName) 
  throws SNEEException, SchemaMetadataException
  {
    super(queryName);
    this.paf = paf;
    this.rt=rt;
  }

  
  
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

  public void setRoot(InstanceOperator rootOp)
  {
    this.rootOp = rootOp;
    instanceOperatorTree.setRoot(rootOp);
  }
  
  public InstanceOperator getRoot() 
  {
    return this.rootOp;
  }
  
  public void addFragment(InstanceFragment frag)
  {
    fragments.add(frag);
  }
  
  public ArrayList<InstanceOperator> getOpInstances(Site site) 
  {
    return this.siteToOpInstMap.get(site);
  }
  
  public void addOpInst(SensornetOperator op, InstanceOperator opInst) 
  {
    instanceOperatorTree.addNode(opInst);
    this.opInstMapping.add(op.getID(), opInst);
  }
  
  public ArrayList<InstanceOperator> getOpInstances(SensornetOperator op) 
  {
    return this.opInstMapping.get(op.getID()); 
  }
  
  public void setOpInstances(SensornetOperator op, Collection<InstanceOperator> opInstColl) 
  {
    this.opInstMapping.set(op.getID(), opInstColl);
  }
  
  public int getNumOpInstances(SensornetOperator op) 
  {
    return this.getOpInstances(op).size();
  }
  
  public void assign(InstanceOperator opInst, Site site) 
  {  
    opInst.setSite(site);
    this.siteToOpInstMap.add(site, opInst);
  }
  
  public void reAssign(InstanceOperator opInst, Site newSite, Site oldSite) 
  {
    opInst.setSite(newSite);
    this.siteToOpInstMap.remove(oldSite, opInst);
    this.siteToOpInstMap.add(newSite, opInst);
  }

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
        
        
        
        
        
        
        
        
        
        
        
        /* proper way to draw edges. but doesn't work here for some reason
        //draw edges
        Iterator<Node> opIterator = instanceOperatorTree.nodeIterator(TraversalOrder.POST_ORDER);
        while (opIterator.hasNext()) {
          InstanceOperator instanceOp = (InstanceOperator) opIterator.next();
          LogicalOperator op = instanceOp.getInstanceOperator().getLogicalOperator();
          Iterator<LogicalOperator> childOpIter = op.childOperatorIterator();
          while (childOpIter.hasNext()) {
            LogicalOperator childOp = childOpIter.next();
            out.print("\"" + childOp.getID() + "\"" + edgeSymbol + "\""
                + op.getID() + "\" ");        
            out.print("[fontsize=9 label = \" ");
            try {
              if (showTupleTypes) {
                out.print("type: " + 
                  childOp.getTupleAttributesStr(3) + " \\n");
              }
            } catch (TypeMappingException e1) {
              String msg = "Problem getting tuple attributes. " + e1;
              logger.warn(msg);
            }
            out.print("\"];\n");
          }
        }
        out.println("}");
        out.close();*/

    } 
    catch (IOException e) 
    {
      System.out.println("Export failed: " + e.toString());
        System.err.println("Export failed: " + e.toString());
    }
  }

  public Iterator<InstanceOperator> iterator(TraversalOrder Order)
  {
    final ArrayList<InstanceOperator> nodeList = 
      new ArrayList<InstanceOperator>();
    this.doIterator(this.getRoot(), nodeList, Order);

    return nodeList.iterator();
  }

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
    }
  }

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
  
  public InstanceFragment getRootFragment()
  {
    return rootFragment;
  }
  
  public void setRootFragment(InstanceFragment rootFragment)
  {
    this.rootFragment = rootFragment;
  }

}
