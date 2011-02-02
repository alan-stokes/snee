package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetNestedLoopJoinOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetNestedLoopJoinOperator.class.getName());
	
	IStreamOperator delOp;
	
	public SensornetNestedLoopJoinOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetNestedLoopJoinOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		delOp = (IStreamOperator) op;
		this.setNesCTemplateName("join");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetNestedLoopJoinOperator()");
		}		
	}

    /** {@inheritDoc} 
     * @throws OptimizationException */
	@Override
	public int getCardinality(CardinalityType card, Site node, DAF daf) throws OptimizationException {
    	int left = getInputCardinality(card, node, daf, 0);
    	int right = getInputCardinality(card, node, daf, 1);
		if (this.getLogicalOperator().getPredicate() instanceof NoPredicate) {
			return (left * right);
		}
        if ((card == CardinalityType.MAX) 
        		|| (card == CardinalityType.PHYSICAL_MAX)) {
			return (left * right);
        } 
        if ((card == CardinalityType.AVERAGE) 
        		|| (card == CardinalityType.MAX)) {
			return (left * right) / Constants.JOIN_PREDICATE_SELECTIVITY;
		}
        if (card == CardinalityType.MINIMUM) {
    		return 0;
        } 
        throw new AssertionError("Unexpected CardinaliyType " + card);
	}

	/** {@inheritDoc} 
	 * @throws OptimizationException */ 
	@Override
    public final int getOutputQueueCardinality(final Site node, final DAF daf) 
	throws OptimizationException {
    	return super.defaultGetOutputQueueCardinality(node, daf);
    }

    /** {@inheritDoc} 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException */    
	@Override
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
    	final int left = getInputCardinality(card, node, daf, 0);
    	final int right = getInputCardinality(card, node, daf, 0);
    	final int tuples = left * right;
    	return getOverheadTimeCost()
    		+ costParams.getCopyTuple() * tuples
    		+ costParams.getApplyPredicate() * tuples;
        }
}
