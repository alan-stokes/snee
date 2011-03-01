package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetRStreamOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;

public class InstanceOperator extends NodeImplementation implements Node
//extends SensornetOperatorImpl
{
  private SensornetOperator instanceOperator;
  private Site site = null;
  private Site getDeepestConfluenceSite;
  private static int counter = 0;
  private float selectivity = 1;
  
  private ArrayList<InstanceOperator> childOps = new ArrayList<InstanceOperator>();
  
  public Iterator<InstanceOperator> iterator()
  {
    return childOps.iterator();
  }

  public boolean add(InstanceOperator e)
  {
    return childOps.add(e);
  }

  public InstanceOperator()
  {
	  super();
  }
  
  public InstanceOperator(SensornetOperator instanceOp, Site site, CostParameters costParams) 
  throws SNEEException, SchemaMetadataException
  {
    super();
    this.instanceOperator = instanceOp;
    this.site = site;
  }
  
  public InstanceOperator(SensornetOperator instanceOp, Site deepestConfluanceSite) 
  {
    super();
    this.instanceOperator = instanceOp;
    this.getDeepestConfluenceSite = deepestConfluanceSite;
    counter ++;
    this.id = generateID(instanceOperator, getDeepestConfluenceSite);
  }
  
  private static String generateID(SensornetOperator op, Site site) 
  {
    StringBuffer id = new StringBuffer();
    id.append(op.getNesCTemplateName().replace(".nc", ""));
    if (site!=null) 
    {
      id.append("_s"+site.getID());
    }
    id.append("_c"+counter);
    return id.toString();
  }
  
  public SensornetOperator getInstanceOperator()
  {
    return instanceOperator;
  }

  public void setInstanceOperator(SensornetOperator instanceOperator)
  {
    this.instanceOperator = instanceOperator;
  }

  public Site getSite()
  {
    return site;
  }

  public void setSite(Site site)
  {
    this.site = site;
  }
  
  public Site getDeepestConfluenceSite()
  {
    return getDeepestConfluenceSite;
  }

  //@Override
  public int[] getSourceSites()
  {
    return instanceOperator.getSourceSites();
  }

  //@Override
  public int getCardinality(CardinalityType card, Site node, DAF daf)
  throws OptimizationException
  {
    return instanceOperator.getCardinality(card, node, daf);
  }

  //@Override
  public int getOutputQueueCardinality(Site node, DAF daf)
  throws OptimizationException
  {
    return instanceOperator.getOutputQueueCardinality(node, daf);
  }

  //@Override
  public int getDataMemoryCost(Site node, DAF daf)
  throws SchemaMetadataException, TypeMappingException, OptimizationException
  {
    return instanceOperator.getDataMemoryCost(node, daf);
  }

  //@Override
  public double getTimeCost(CardinalityType card, Site node, DAF daf)
  throws OptimizationException
  {
    return instanceOperator.getTimeCost(card, node, daf);
  }

  public InstanceOperator getIInput(int index)
  {
    return (InstanceOperator)instanceOperator.getInput(index);
  }
  
  /**
   * code used to find if a node is dead
   */
  public boolean isNodeDead()
  {
    return site.isDead();
  }
  
  /**
   * used to calculate if an instance oeprator is on the same site as given instance operator
   */
  public boolean isRemote(InstanceOperator instance)
  {
    if(this.site == instance.getSite())
      return true;
    else
      return false;    
  }
  
  /**
   * selectivity calculation which determines how many tuples would pass a given run
   * @return selectivity value ranging between 0 and 1 where 0 is no tuples pass and 1 is all
   */
  public float selectivity()
  {
    /**
     * requires use of expression based predicate at some point, but currently maximum 
     * selectivity is assumed
     */
    return selectivity;
  }
  
}
