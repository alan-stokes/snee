package uk.ac.manchester.cs.snee.compiler.sn.physical;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.evaluator.QueryEvaluator;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;

public class AlgorithmSelector {

	private Logger logger = 
		Logger.getLogger(AlgorithmSelector.class.getName());
	
	public PAF doPhysicalOptimizaton(DLAF dlaf, CostParameters costParams, 
	String queryName) 
	throws SNEEException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER getInstance() with " + dlaf.getID());
		LogicalOperator rootOp = dlaf.getLAF().getRootOperator();
		
		SensornetOperator deliverPhyOp = null;
		/* Query plans must have a deliver operator at their root */
		if (rootOp instanceof DeliverOperator) {
			deliverPhyOp = new SensornetDeliverOperator(rootOp, costParams);
		} else {
			String msg = "Unsupported operator " + rootOp.getOperatorName() +
				". Query plans should have a DeliverOperator as their root.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		PAF paf = new PAF(deliverPhyOp, dlaf, costParams, queryName);
		splitAggregationOperators(paf, costParams);
		removeNOWwindows(paf);
		return paf;
	}
	
    private void removeNOWwindows(PAF paf) {
		final Iterator<SensornetOperator> opIter = paf
		.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = (SensornetOperator) opIter.next();
		    if (op instanceof SensornetWindowOperator) {
		    	SensornetWindowOperator winOp = (SensornetWindowOperator)op;
		    	if (winOp.getFrom()==0 && winOp.getTo()==0 && winOp.isTimeScope()) {
		    		try {
						paf.getOperatorTree().removeNode(winOp);
					} catch (OptimizationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	}
		    }
		}		
	}

	/**
     * Splits Aggregation operators into two operators, 
     * to allow incremental aggregation.
     * @param paf the physical algebra format.
     * @throws SchemaMetadataException 
     * @throws SNEEException 
     */
    private void splitAggregationOperators(final PAF paf, CostParameters costParams) 
    throws SNEEException, SchemaMetadataException {

		final Iterator<SensornetOperator> opIter = paf
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
		    	if (agg.isSplittable()) {
			    	AggregationOperator logAggr = 
			    		(AggregationOperator) agg.getLogicalOperator();
					SensornetAggrInitOperator aggrInit = 
						new SensornetAggrInitOperator(logAggr, costParams);
					SensornetAggrMergeOperator aggrMerge = 
						new SensornetAggrMergeOperator(logAggr, costParams);
					SensornetAggrEvalOperator aggrEval = 
						new SensornetAggrEvalOperator(logAggr, costParams);
					
					SensornetOperator childOp = agg.getLeftChild();
					SensornetOperator parentOp = agg.getParent();
					try {
						paf.getOperatorTree().removeNode(agg);
					} catch (OptimizationException e) {
						// TODO Auto-generated catch block
						System.exit(0);
					}
					paf.getOperatorTree().insertNode(childOp, parentOp, aggrInit);
					paf.getOperatorTree().insertNode(aggrInit, parentOp, aggrMerge);
					paf.getOperatorTree().insertNode(aggrMerge, parentOp, aggrEval);		    		
		    	}		    	
		    }
		}
    }

}
