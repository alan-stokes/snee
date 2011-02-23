package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class SensornetIncrementalAggregationOperator extends SensornetOperatorImpl {

	private static Logger logger 
	= Logger.getLogger(SensornetIncrementalAggregationOperator.class.getName());
	
	AggregationOperator aggrOp;
	
	public SensornetIncrementalAggregationOperator(LogicalOperator op,
	CostParameters costParams) throws SNEEException,
			SchemaMetadataException {
		super(op, costParams, false);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetIncrementalAggregationOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		aggrOp = (AggregationOperator) op;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetIncrementalAggregationOperator()");
		}		
	}

	/** {@inheritDoc} */
	public int getCardinality(CardinalityType card, 
			Site node, DAF daf) {
		return 1;
	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getCardinality(CardinalityType card, 
//			Site node, DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(1);
//    	return result;
//    }


///** {@inheritDoc} */
//public double getTimeCost(CardinalityType card, int numberOfInstances) {
//	int tuples = this.getInputCardinality(card, 0, numberOfInstances);
//	return getTimeCost(tuples);
//}

///** {@inheritDoc} */
//public AlphaBetaExpression getTimeExpression(
//		CardinalityType card, Site node, 
//		DAF daf, boolean round) {
//	AlphaBetaExpression result = new AlphaBetaExpression();
//	result.addBetaTerm(getOverheadTimeCost() + CostParameters.getCopyTuple());
//	AlphaBetaExpression tuples 
//		= this.getInputCardinality(card, node, daf, round, 0);
//	tuples.multiplyBy(CostParameters.getDoCalculation());
//	result.add(tuples);
//	return result;
//}

	/** {@inheritDoc} 
	 * @throws OptimizationException */    
	public final int getOutputQueueCardinality(final Site node, final DAF daf) 
	throws OptimizationException {
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
	
	@Override
	/** {@inheritDoc} */
    public final int[] getSourceSites() {
    	return super.defaultGetSourceSites();
    }

	//delegate
	public List<AggregationExpression> getAggregates() {
		return this.aggrOp.getAggregates();
	}
	
}
