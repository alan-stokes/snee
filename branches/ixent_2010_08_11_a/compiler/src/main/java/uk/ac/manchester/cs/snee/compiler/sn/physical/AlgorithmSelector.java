package uk.ac.manchester.cs.snee.compiler.sn.physical;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.evaluator.QueryEvaluator;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSingleStepAggregationOperator;

public class AlgorithmSelector {

	private Logger logger = 
		Logger.getLogger(AlgorithmSelector.class.getName());
	
	public PAF doPhysicalOptimizaton(DLAF dlaf, String queryName) 
	throws SNEEException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER getInstance() with " + dlaf.getName());
		LogicalOperator rootOp = dlaf.getRootOperator();
		
		SensornetOperator deliverPhyOp = null;
		/* Query plans must have a deliver operator at their root */
		if (rootOp instanceof DeliverOperator) {
			deliverPhyOp = new SensornetDeliverOperator(rootOp);
		} else {
			String msg = "Unsupported operator " + rootOp.getOperatorName() +
				". Query plans should have a DeliverOperator as their root.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		PAF paf = new PAF(deliverPhyOp, dlaf, queryName);
		return paf;
	}
	
    /**
     * Splits Aggregation operators into two operators, 
     * to allow incremental aggregation.
     * @param paf the physical algebra format.
     * @throws SchemaMetadataException 
     * @throws SNEEException 
     */
    private void splitAggregationOperators(final PAF paf) 
    throws SNEEException, SchemaMetadataException {

		final Iterator<LogicalOperator> opIter = paf
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = (SensornetOperator) opIter.next();
	
		    //TODO: Only split the aggregation operator if the function will
		    //yield efficiencies, e.g., it may not be worthwhile to split an
		    //operator in the case of a median, because
		    //it can't be incrementally computed
		    if (op instanceof SensornetSingleStepAggregationOperator) {
				//Split into three
		    	SensornetSingleStepAggregationOperator agg = 
		    		(SensornetSingleStepAggregationOperator) op;
		    	AggregationOperator logAggr = 
		    		(AggregationOperator) agg.getLogicalOp();
				SensornetAggrInitOperator aggrInit = 
					new SensornetAggrInitOperator(logAggr);
				SensornetAggrMergeOperator aggrMerge = 
					new SensornetAggrMergeOperator(logAggr);
				SensornetAggrEvalOperator aggrEval = 
					new SensornetAggrEvalOperator(logAggr);
				paf.replacePath(op, new Node[] { aggrEval, aggrInit });
				paf.insertNode(aggrInit, aggrEval, aggrMerge);
		    }
		}
    }

}
