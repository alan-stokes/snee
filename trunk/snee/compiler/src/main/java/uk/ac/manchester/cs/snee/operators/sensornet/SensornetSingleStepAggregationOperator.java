package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetSingleStepAggregationOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetSingleStepAggregationOperator.class.getName());
	
	AggregationOperator aggrOp;
	
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
}
