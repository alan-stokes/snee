package uk.ac.manchester.cs.snee.compiler.iot;

import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class IOTUtils
{
  private IOT iot;
  static private DAF daf;
  private CostParameters costs;
  
  public IOTUtils(final IOT iot, CostParameters costParams) 
  {
    this.iot = iot;
    this.costs = costParams;
  }
  
  public void convertToDAF() throws OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException
  {
    createDAF();
  }
  
  private void createDAF() 
  throws OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException
  {
    DAF faf = partitionPAF(iot.getPaf(), iot, iot.getRT().getQueryName(), iot.getRT(), costs);  
    daf = linkFragments(faf, iot.getRT(), iot, iot.getRT().getQueryName());
    removeRedundantAggrIterOp(daf, iot);
  }
  
  private static DAF partitionPAF(final PAF paf, IOT oit, 
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
    while (opIter.hasNext()) 
    {
      final SensornetOperator op = (SensornetOperator) opIter.next();
      HashSet<Site> opSites = oit.getSites(op);    
      
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
          
          HashSet<Site> childSites = oit.getSites(childOp);
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
  
  private static DAF linkFragments(DAF faf, RT rt, IOT iot,
      String queryName) throws SNEEException, SchemaMetadataException {
    DAF cDAF = faf;
    
    Iterator<InstanceOperator> opInstIter = iot.iterator(TraversalOrder.POST_ORDER);
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
  
  public DAF getDAF()
  {
    return daf;
  }
}
