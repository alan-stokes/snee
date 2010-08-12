package uk.ac.manchester.cs.snee.compiler.sn.physical;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.evaluator.QueryEvaluator;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class AlgorithmSelector {

	private Logger logger = 
		Logger.getLogger(AlgorithmSelector.class.getName());
	
	public PAF doPhysicalOptimizaton(DLAF dlaf, String queryName) 
	throws SNEEException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER getInstance() with " + dlaf.getName());
		LogicalOperator rootOp = dlaf.getRootOperator();
		
		SensornetOperator phyOp = null;
		/* Query plans must have a deliver operator at their root */
		if (rootOp instanceof DeliverOperator) {
			phyOp = new SensornetDeliverOperator(rootOp);
		} else {
			String msg = "Unsupported operator " + rootOp.getOperatorName() +
				". Query plans should have a DeliverOperator as their root.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		PAF paf = new PAF(dlaf, queryName);
		return paf;
	}

}
