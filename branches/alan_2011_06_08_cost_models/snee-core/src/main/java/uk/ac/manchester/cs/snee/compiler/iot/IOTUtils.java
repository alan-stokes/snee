package uk.ac.manchester.cs.snee.compiler.iot;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class IOTUtils
{
  private final IOT iot;
  private Tree instanceOperatorTree = null;
  private DAF daf;
  private static CostParameters costs;
  
  public IOTUtils(final IOT newIOT, CostParameters costParams) 
  {
    this.iot = newIOT;
    IOTUtils.costs = costParams;
    this.instanceOperatorTree = this.iot.getOperatorTree();
  }
  
  public IOTUtils()
  {
    iot = null;
  }
  
  public DAF convertToDAF() throws OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException
  {
    createDAF();
    return daf;
  }

  public DAF getDAF()
  {
    return iot.getDAF();
  }
  
  public void disconnectExchanges()
  {
    Iterator<InstanceFragment> fragIterator = iot.getInstanceFragments().iterator();
    while(fragIterator.hasNext())
    {
      InstanceFragment frag = fragIterator.next();
      if(iot.getRT().getSite(frag.getSite().getID()) != null)
      {
        InstanceOperator rootOp = frag.getRootOperator();
        assert !(rootOp.getSensornetOperator() instanceof SensornetExchangeOperator);
        if(!(rootOp.getSensornetOperator() instanceof SensornetDeliverOperator))
        {
          if((rootOp.getOutput(0) instanceof InstanceExchangePart))
          {
            InstanceExchangePart exchange = (InstanceExchangePart) rootOp.getOutput(0);
            InstanceExchangePart lastExchange = exchange;
            while(lastExchange.getNext() != null)
            {
              lastExchange = lastExchange.getNext();
            }
            InstanceOperator lowestOp = exchange.getDestFrag().getLowestOperator();
            rootOp.setOutput(lowestOp, 0);
            lowestOp.replaceInputByID(lastExchange, rootOp);
          }
        }
      }
    }
  }
  
  public void reconnectExchanges() 
  {
    Iterator<InstanceFragment> fragmentIterator = iot.instanceFragmentIterator(TraversalOrder.POST_ORDER);
    //go though fragments
    while(fragmentIterator.hasNext())
    {
      InstanceFragment currentFrag = fragmentIterator.next();//if frag on site
      //get root operator
      InstanceOperator rootOp = currentFrag.getRootOperator();
      if(!(rootOp.getSensornetOperator() instanceof SensornetDeliverOperator))
      {
        InstanceExchangePart exchange = currentFrag.getParentExchangeOperator();
        //always have 1 output
        rootOp.replaceOutput(rootOp.getOutput(0), exchange);
        while(exchange.getNext() != null)
        {
          InstanceExchangePart nextExchange = exchange.getNext(); 
          exchange = nextExchange;  
        }
        //get root operator of higher frag frag
        InstanceOperator lowestOperator = exchange.getDestFrag().getLowestOperator();
        lowestOperator.replaceInputByID(rootOp, exchange);   
      }
    }
  }
  
  private void createDAF() 
  throws OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException
  {
    DAF faf = partitionPAF(iot.getPAF(), iot, iot.getRT().getQueryName(), iot.getRT(), costs);  
    daf = linkFragments(faf, iot.getRT(), iot, iot.getRT().getQueryName());
    removeRedundantAggrIterOp(daf, iot);
    removeRedundantExchanges(daf);
    updateFragments(daf);
    updateSites(daf);
  }
  
  /**
   * removes the exchanges from the paf added by the DAF generator
   * @param paf
   * @throws OptimizationException 
   */
  public void removeExchangesFromPAF(PAF paf) throws OptimizationException
  {
    Iterator<SensornetOperator> opIter = paf.operatorIterator(TraversalOrder.POST_ORDER);
    /*iterate though physical operators looking at each and removing the exchanges */
    while (opIter.hasNext())
    {
      SensornetOperator op = opIter.next();
      if(!(op instanceof SensornetDeliverOperator))
      {
        if(op.getOutput(0) instanceof SensornetExchangeOperator)
        {
          SensornetOperator nextOp = (SensornetOperator) op.getOutput(0);
          while(nextOp instanceof SensornetExchangeOperator)
          {
            paf.getOperatorTree().removeNode(nextOp);
            nextOp = (SensornetOperator) nextOp.getOutput(0);
          }
        }
      }
    }
  }
  
  private void updateFragments(DAF daf2)
  {
    Iterator<SensornetOperator> opIter = daf.operatorIterator(TraversalOrder.PRE_ORDER);
    while (opIter.hasNext()) 
    {
      SensornetOperator op = opIter.next();
      if (op instanceof SensornetExchangeOperator) 
      {
        SensornetOperator nextOp = opIter.next();
        Iterator<Fragment> fragIterator = daf.getFragments().iterator();
        while(fragIterator.hasNext())
        {
          Fragment frag = fragIterator.next();
          if(frag.getOperators().contains(nextOp))
          {
            frag.setParentExchange((SensornetExchangeOperator) op);
          } 
        }
      }
    }
  }

  /**
   * this method instills the fragments onto the sites, this is to allow the iot to be compatible with the code generator.
   * @param daf
   */
  private void updateSites(DAF daf)
  {
    Iterator<Fragment> fragIterator = daf.fragmentIterator(TraversalOrder.POST_ORDER);
    while(fragIterator.hasNext())
    {
      Fragment frag = fragIterator.next();
      ArrayList<Site> fragSites = frag.getSites();
      Iterator<Site> fragSiteIterator = fragSites.iterator();
      while(fragSiteIterator.hasNext())
      {
        Site currentSite = fragSiteIterator.next();
        currentSite = daf.getRT().getSite(currentSite.getID());
        currentSite.addFragment(frag);
      }
    }
    
  }

  private static DAF partitionPAF(final PAF paf, IOT iot, 
                                  final String queryName, RT routingTree,
                                  CostParameters costs) 
  throws SNEEException, SchemaMetadataException 
  {
    DAF faf = new DAF(paf, routingTree, queryName);
    
    //Get rid of unnecessary aggrIter in FAF... (i.e., they have not been assigned to any site)
    Iterator<SensornetOperator> opIter = faf
    .operatorIterator(TraversalOrder.POST_ORDER);
    while (opIter.hasNext()) {
      final SensornetOperator op = (SensornetOperator) opIter.next();
      HashSet<String> opSites = iot.getSites(op);
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
    while (opIter.hasNext()) 
    {
      final SensornetOperator op = (SensornetOperator) opIter.next();
      HashSet<String> opSites = iot.getSites(op);    
      
      if(op instanceof SensornetAggrEvalOperator)
      {
        final SensornetOperator childOp = (SensornetOperator) op.getInput(0);
        final SensornetExchangeOperator exchOp = new SensornetExchangeOperator(costs);
        faf.getOperatorTree().insertNode(childOp, op, exchOp);        
      }
      else if (op instanceof SensornetAggrMergeOperator) 
      {
        final SensornetOperator childOp = (SensornetOperator) op.getInput(0);
        final SensornetExchangeOperator exchOp = new SensornetExchangeOperator(costs);
        faf.getOperatorTree().insertNode(childOp, op, exchOp);        
      } else {
        for (int i=0; i<op.getInDegree(); i++) 
        {
          final SensornetOperator childOp = (SensornetOperator) op.getInput(i);
          
          HashSet<String> childSites = iot.getSites(childOp);
          if (!opSites.equals(childSites)) 
          {
            final SensornetExchangeOperator exchOp = new SensornetExchangeOperator(costs);   
            faf.getOperatorTree().insertNode(childOp, op, exchOp);
          }
        }
      }
    }

    faf.buildFragmentTree();
    return faf;
  }
  
  private static DAF linkFragments(DAF faf, RT rt, IOT iot, String queryName) 
  throws SNEEException, SchemaMetadataException 
  {
    DAF cDAF = faf;
    Iterator<InstanceOperator> opInstIter = iot.treeIterator(TraversalOrder.POST_ORDER);
    while (opInstIter.hasNext()) 
    {
      InstanceOperator opInst = opInstIter.next();
      if(!(opInst instanceof InstanceExchangePart))
      {
        //have to get the cloned copy in compactDaf...
        SensornetOperator op = (SensornetOperator)cDAF.getOperatorTree().getNode(opInst.getSensornetOperator().getID());

        Site sourceSite = (Site)cDAF.getRT().getSite(opInst.getSite().getID());
        Fragment sourceFrag = op.getContainingFragment();
      
        if (op.getOutDegree() > 0) {
          SensornetOperator parentOp = op.getParent();

          if (parentOp instanceof SensornetExchangeOperator) 
          {
            InstanceOperator paOpInst = (InstanceOperator)opInst.getOutput(0);
            while(paOpInst instanceof InstanceExchangePart)
            {
              paOpInst = (InstanceOperator)paOpInst.getOutput(0);
            }
          
            Site destSite = (Site)cDAF.getRT().getSite(paOpInst.getSite().getID());
            String OpInstOutputPhysicalID = paOpInst.getSensornetOperator().getID();
            SensornetOperator dafSensorNetOperator = (SensornetOperator)cDAF.getOperatorTree().getNode(OpInstOutputPhysicalID);
            Fragment destFrag = dafSensorNetOperator.getContainingFragment();
            if(sourceSite == null)
              System.out.println(opInst.getSite().getID() + " to " + paOpInst.getSite().getID()  + " source failed");
            if(destSite == null)
              System.out.println(opInst.getSite().getID() + " to " + paOpInst.getSite().getID() + " dest failed");
           // System.out.println("path between "+ sourceSite.getID() +  " and " + destSite.getID());
            final Path path = cDAF.getRT().getPath(sourceSite.getID(), destSite.getID());
          
            cDAF.placeFragment(sourceFrag, sourceSite);
            cDAF.linkFragments(sourceFrag, sourceSite, destFrag, destSite, path);
          }       
        } 
        else 
        {  
        cDAF.placeFragment(sourceFrag, sourceSite);
        }
      }
     }
    return cDAF;
  }

  public void removeRedundantExchanges(DAF daf) throws OptimizationException {
    HashSet<SensornetExchangeOperator> exchangesToBeRemoved =
      new HashSet<SensornetExchangeOperator>(); 
    
    Iterator<SensornetOperator> opIter = daf.operatorIterator(TraversalOrder.PRE_ORDER);
    while (opIter.hasNext()) {
      SensornetOperator op = opIter.next();
      if (op instanceof SensornetExchangeOperator) {
        SensornetExchangeOperator exchOp = (SensornetExchangeOperator)op;
        Fragment sourceFrag = exchOp.getSourceFragment();
        ArrayList<Site> sourceSites = sourceFrag.getSites();
        Fragment destFrag = exchOp.getDestFragment();
        ArrayList<Site> destSites = destFrag.getSites();
        
        if (sourceSites.equals(destSites) || sourceSites.size() == 0) {
          exchangesToBeRemoved.add(exchOp);
        }
      }
    }
    
    Iterator<SensornetExchangeOperator> exchOpIter = exchangesToBeRemoved.iterator();
    while (exchOpIter.hasNext()) {
      SensornetExchangeOperator exchOp = exchOpIter.next();
      this.removeExchangeOperator(exchOp, daf);
    }
  }
  
  public void removeExchangeOperator(SensornetExchangeOperator exchOp, DAF daf) throws OptimizationException {
    //Merge the source fragment operators into the destination fragment     
    Fragment sourceFrag = exchOp.getSourceFragment();
    Fragment destFrag = exchOp.getDestFragment();
    
    if(sourceFrag != null)
    {
        
      destFrag.mergeChildFragment(sourceFrag);
      
      //Remove the exchange operator and fragment
      daf.removeNode(exchOp);
      daf.removeFragment(sourceFrag);
      
      Iterator<Site> siteIter = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
      while (siteIter.hasNext()) {
        Site site = siteIter.next();
      
        //Remove the exchange components from any routing tree sites
        site.removeExchangeComponents(exchOp);
        
        //Remove the source fragment from any routing tree sites
        site.removeFragment(sourceFrag);
        
        //Change the destination fragment on any exchange component which has the 
        //old destination fragment
        Iterator<ExchangePart> exchCompIter = site.getExchangeComponents().iterator();
        while (exchCompIter.hasNext()) {
          ExchangePart exchComp = exchCompIter.next();
          if (exchComp.getDestFrag()==sourceFrag) {
            exchComp.setDestFrag(destFrag);
          }
        }
      }
    }
    else
    {
      daf.removeNode(exchOp);
    }
  }
  
  
  private static void removeRedundantAggrIterOp(DAF daf, IOT instanceDAF) throws OptimizationException {

    Iterator<SensornetOperator> opIter = daf.operatorIterator(TraversalOrder.POST_ORDER);
    while (opIter.hasNext()) {
      SensornetOperator op = opIter.next();
      if (op instanceof SensornetAggrMergeOperator) {
        if (!(op.getParent() instanceof SensornetExchangeOperator) &&
            instanceDAF.getOpInstances(op).size() != 1
            ) {
          daf.getOperatorTree().removeNode(op);
        }
      }
    }
  }
  
  /**
   * produce a image of the IOT as a dot file
   * @param fname name of file
   * @param label label for within the dot file.
   * @throws SchemaMetadataException
   */
  public void exportAsDOTFile(final String fname, final String label, boolean exchangesOnSites) 
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
        final Iterator<Site> siteIter = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
        while (siteIter.hasNext()) 
        {
          final Site site = siteIter.next();
          ArrayList<InstanceOperator> opInstSubTree = iot.getOpInstances(site);
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
        
        
          //add exchanges if needed
          if(exchangesOnSites)
          {
            HashSet<InstanceExchangePart> exchangeParts = site.getInstanceExchangeComponents();
            Iterator<InstanceExchangePart> exchangePartsIterator = exchangeParts.iterator();
            while(exchangePartsIterator.hasNext())
            {
              InstanceExchangePart exchangePart = exchangePartsIterator.next();
              out.println("\"" + exchangePart.getID() + "\" ;");
            }
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
   * @param outputFolderString  name of file
   * @param label label for within dot file
   * @param exchangesOnSites if exchange operators are broken down
   */
  public void exportAsDotFileWithFrags(final String outputFolderString, final String label, boolean exchangesOnSites)
  {
    exportAsDotFileWithFrags(outputFolderString, label, exchangesOnSites, false);
  }

  public void exportAsDotFileWithFrags(final String outputFolderString, final String label,
                                       boolean exchangesOnSites, boolean inputsOutputsSites)
  {
    try
    {
      //create writer
      PrintWriter out = new PrintWriter(new BufferedWriter( new FileWriter(outputFolderString)));
      //create blurb
      out.println("digraph \"" + (String) instanceOperatorTree.getName() + "\" {");
      out.println("label = \"" + label + "\";");
      out.println("rankdir=\"BT\";");
      out.println("compound=\"true\";");
      //itterate though sites in routing tree (depth to shallow)
      //Iterator<Site> siteIterator = iot.siteIterator();
      Iterator<Site> siteIterator = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
      while(siteIterator.hasNext())
      {//for each site do blurb and then go though fragments
        Site currentSite = siteIterator.next();
        out.println("subgraph cluster_s" + currentSite.getID() + " {");
        out.println("style=\"rounded,dotted\"");
        out.println("color=blue;");
        //get fragments within site
        Iterator<InstanceFragment> fragmentIterator = iot.getInstanceFragments().iterator();
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
              if(!inputsOutputsSites)
                out.println("\"" + currentOperator.getID() + "\" ;");
              else
              {
                String output = "\"" + currentOperator.getID() + "\" [label = \"" + 
                " (" + currentOperator.getSite().getID() + ")(";
                String inputs = "";
                Iterator<Node> inputIterator = currentOperator.getInputsList().iterator();
                while(inputIterator.hasNext())
                {
                  InstanceOperator input = (InstanceOperator) inputIterator.next();
                  inputs = inputs.concat(input.getSite().getID() + ",");
                }
                output = output.concat(inputs + ")(");
                if(currentOperator.getOutDegree() != 0)
                {
                  InstanceOperator outputop = (InstanceOperator) currentOperator.getOutput(0);
                  output = output.concat(outputop.getSite().getID());
                }
                output = output.concat(")\"] ;");
                out.println(output);
              }
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
            if(!inputsOutputsSites)
              out.println("\"" + exchangePart.getID() + "\" ;");
            else
            {
              String output = "\"" + exchangePart.getID() + "\" [label = \"" +
              " (" + exchangePart.getSite().getID() + ")(";
              String inputs = "";
              Iterator<Node> inputIterator = exchangePart.getInputsList().iterator();
              while(inputIterator.hasNext())
              {
                InstanceOperator input = (InstanceOperator) inputIterator.next();
                inputs = inputs.concat(input.getSite().getID() + ",");
              }
              output = output.concat(inputs + ")(");
              InstanceOperator outputop = (InstanceOperator) exchangePart.getOutput(0);
              output = output.concat(outputop.getSite().getID());
              output = output.concat(") \" ] ;");
              out.println(output);
            }
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
}
