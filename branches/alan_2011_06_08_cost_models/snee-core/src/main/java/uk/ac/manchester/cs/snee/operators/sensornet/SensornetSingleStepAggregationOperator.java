package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetSingleStepAggregationOperator extends SensornetOperatorImpl {
	
	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -1936039365372759674L;

  private static final Logger logger = Logger.getLogger(SensornetSingleStepAggregationOperator.class.getName());
	
	AggregationOperator aggrOp;
	
	ArrayList<Attribute> incrAggrAttributes;

	public SensornetSingleStepAggregationOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetSingleStepAggregationOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		aggrOp = (AggregationOperator) op;
		this.setNesCTemplateName("aggregation");
		incrAggrAttributes = SensornetAggrInitOperator.getIncrementalAggregationAttributes(aggrOp);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetSingleStepAggregationOperator()");
		}		
	}

	/** {@inheritDoc} */
	public int getCardinality(CardinalityType card, 
			Site node, DAF daf) {
		return 1;
	}
	
	/** {@inheritDoc} 
	 * @throws OptimizationException */    
	public final int getOutputQueueCardinality(final Site node, final DAF daf) throws OptimizationException {
		return super.defaultGetOutputQueueCardinality(node, daf);
	}
	
	/** {@inheritDoc} 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}
	
	/** {@inheritDoc} */
    public final int[] getSourceSites() {
    	return super.defaultGetSourceSites();
    }
	
    /** {@inheritDoc} 
     * @throws OptimizationException */
	public final double getTimeCost(final CardinalityType card,
			final Site node, final DAF daf) throws OptimizationException {
		final int tuples = this.getInputCardinality(card, node, daf, 0);
		return getOverheadTimeCost()
				+ costParams.getCopyTuple() 
				+ costParams.getDoCalculation() * tuples;
	}

	public boolean isSplittable() {
		return aggrOp.isSplittable();
	}
	
	//delegate except for exchange operators or aggregates
	public List<Attribute> getAttributes() {
		ArrayList<Attribute> outputAttributes = new ArrayList<Attribute>();
		
		for (Attribute attr : this.getLogicalOperator().getAttributes()) {
			outputAttributes.add(attr);
		}
		return outputAttributes;
	}
	
	
	public ArrayList<Attribute> getIncrAggrAttributes() {
		return incrAggrAttributes;
	}
}
