package uk.ac.manchester.cs.snee.compiler.iot;

import java.util.ArrayList;

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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class InstanceOperator extends NodeImplementation implements Node
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -4582709096587603421L;
  private SensornetOperator sensornetOperator;
  private Site site = null;
  private Site getDeepestConfluenceSite;
  private static int counter = 0;
  private float selectivity = 1;
  private InstanceFragment corraspondingFragment = null;
  /**
   * used to determine number of tuples from packets
   */
  private Integer lastPacketTupleCount = 0;

  public InstanceOperator()
  {
	  super();
  }
  
  public InstanceOperator(SensornetOperator instanceOp, Site site, CostParameters costParams) 
  throws SNEEException, SchemaMetadataException
  {
    super();
    this.sensornetOperator = instanceOp;
    this.site = site;
  }
  
  public InstanceOperator(SensornetOperator instanceOp, Site deepestConfluanceSite) 
  {
    super();
    this.sensornetOperator = instanceOp;
    this.getDeepestConfluenceSite = deepestConfluanceSite;
    counter ++;
    this.id = generateID(sensornetOperator, getDeepestConfluenceSite);
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
  
  public SensornetOperator getSensornetOperator()
  {
    return sensornetOperator;
  }
  
  public void setSensornetOperator(SensornetOperator op)
  {
    this.sensornetOperator = op;
  }

  public void setInstanceOperator(SensornetOperator instanceOperator)
  {
    this.sensornetOperator = instanceOperator;
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
  
  public void setDeepestConfluenceSite(Site newSite)
  {
    getDeepestConfluenceSite = newSite;
    this.id = generateID(sensornetOperator, getDeepestConfluenceSite);
  }

  //@Override
  public ArrayList<Integer> getSourceSites()
  {
    return sensornetOperator.getSourceSites();
  }

  //@Override
  public int getCardinality(CardinalityType card, Site node, DAF daf)
  throws OptimizationException
  {
    return sensornetOperator.getCardinality(card, node, daf);
  }

  //@Override
  public int getOutputQueueCardinality(Site node, DAF daf)
  throws OptimizationException
  {
    return sensornetOperator.getOutputQueueCardinality(node, daf);
  }

  //@Override
  public int getDataMemoryCost(Site node, DAF daf)
  throws SchemaMetadataException, TypeMappingException, OptimizationException
  {
    return sensornetOperator.getDataMemoryCost(node, daf);
  }

  //@Override
  public double getTimeCost(CardinalityType card, Site node, DAF daf)
  throws OptimizationException
  {
    return sensornetOperator.getTimeCost(card, node, daf);
  }
  
  public InstanceOperator getInstanceInput(int index)
  {
    return (InstanceOperator)this.getInput(index);
  }
  
  /**
   * code used to find if a node is dead
   */
  public boolean isNodeDead()
  {
    return site.isDeadInSimulation();
  }
  
  /**
   * used to calculate if an instance operator is on the same site as given instance operator
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
 
  /**
   * over load method so input node also adds to childOps
   */
  public void addInput(Node n)
  {
    super.addInput(n);
  }
  
  /**
   * overload method so remove input also removes from childOps
   */
  public void removeInput(Node n)
  {
    super.removeInput(n);
  }
  
  public void removeAllInputs()
  {
    super.clearInputs();
  }

  public void setCorraspondingFragment(InstanceFragment corraspondingFragment)
  {
    this.corraspondingFragment = corraspondingFragment;
  }

  public InstanceFragment getCorraspondingFragment()
  {
    return corraspondingFragment;
  }
  
  public void setLastPacketTupleCount(Integer lastPacketTupleCount)
  {
    this.lastPacketTupleCount = lastPacketTupleCount;
  }

  public Integer getLastPacketTupleCount()
  {
    return lastPacketTupleCount;
  }

  public String toString()
  {
    return this.id;
  }
 
}
