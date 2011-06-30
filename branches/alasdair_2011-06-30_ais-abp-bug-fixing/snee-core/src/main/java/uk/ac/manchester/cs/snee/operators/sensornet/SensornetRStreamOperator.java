package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;

public class SensornetRStreamOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetRStreamOperator.class.getName());
	
	RStreamOperator rStrOp;
	
	public SensornetRStreamOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetRStreamOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		rStrOp = (RStreamOperator) op;	
		this.setNesCTemplateName(null);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetRStreamOperator()");
		}		
	}

	@Override
	/** {@inheritDoc} */
	public final int getCardinality(final CardinalityType card, 
			final Site node, final DAF daf) throws OptimizationException {
		return getInputCardinality(card, node, daf, 0);
	}

	@Override
	/** {@inheritDoc} */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {	
		return super.defaultGetDataMemoryCost(node, daf);
	}

	@Override
	/** {@inheritDoc} */    
    public final int getOutputQueueCardinality(final Site node, final DAF daf) 
	throws OptimizationException {
    	return super.defaultGetOutputQueueCardinality(node, daf);
    }

    /** {@inheritDoc} */
    public final int[] getSourceSites() {
    	return super.defaultGetSourceSites();
    }
	
	/** {@inheritDoc} */
    public final double getTimeCost(final CardinalityType card, 
		   final Site node, final DAF daf) {
		return this.getOverheadTimeCost();
    }
	
}
