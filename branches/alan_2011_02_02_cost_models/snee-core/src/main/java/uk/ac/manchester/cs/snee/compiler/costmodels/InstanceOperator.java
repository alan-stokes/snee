package uk.ac.manchester.cs.snee.compiler.costmodels;

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
//extends SensornetOperatorImpl
{
  private SensornetOperator instanceOperator;
  private Site site = null;
  private Site getDeepestConfluenceSite;
  private static int counter = 0;

  private InstanceOperator nextExchange;
  private InstanceOperator previousExchange;
  
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

  public InstanceOperator getNextExchange()
  {
    return nextExchange;
  }

  public void setNextExchange(InstanceOperator nextExchange)
  {
    this.nextExchange = nextExchange;
  }

  public InstanceOperator getPreviousExchange()
  {
    return previousExchange;
  }

  public void setPreviousExchange(InstanceOperator previousExchange)
  {
    this.previousExchange = previousExchange;
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

  //@Override
  public float getInstanceCardinality(Site node, InstanceDAF instanceDAF, long beta)
  throws OptimizationException
  {
    if(isNodeDead())
      return 0;
    else
      return instanceOperator.getInstanceCardinality(node, instanceDAF, beta);
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
  
  
}
