package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;

public class SensornetSelectOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetSelectOperator.class.getName());
	
	SelectOperator selOp;
	
	public SensornetSelectOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetSelectOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		selOp = (SelectOperator) op;
		this.setNesCTemplateName("select");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetSelectOperator()");
		}		
	}

	//@Override
	/** {@inheritDoc} */
	public final int getCardinality(final CardinalityType card, 
			final Site node, final DAF daf) throws OptimizationException {
		int input = getInputCardinality(card, node, daf, 0);
        if ((card == CardinalityType.MAX) 
        		|| (card == CardinalityType.PHYSICAL_MAX)) {
			return input;
        } 
        if ((card == CardinalityType.AVERAGE) 
        		|| (card == CardinalityType.MAX)) {
			return input / Constants.JOIN_PREDICATE_SELECTIVITY;
		}
        if (card == CardinalityType.MINIMUM) {
    		return 0;
        } 
        throw new AssertionError("Unexpected CardinaliyType " + card);
	}

	//@Override
	/** {@inheritDoc} */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}

	//@Override
	/** {@inheritDoc} */    
    public final int getOutputQueueCardinality(final Site node, final DAF daf) throws OptimizationException {
    	return super.defaultGetOutputQueueCardinality(node, daf);
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
			+ (costParams.getCopyTuple() 
			+ costParams.getApplyPredicate()) * tuples;
    }
	
}
