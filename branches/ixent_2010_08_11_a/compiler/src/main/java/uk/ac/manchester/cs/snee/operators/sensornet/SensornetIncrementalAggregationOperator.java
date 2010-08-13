package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class SensornetIncrementalAggregationOperator extends SensornetOperatorImpl {

	private static Logger logger 
	= Logger.getLogger(SensornetIncrementalAggregationOperator.class.getName());
	
	AggregationOperator aggrOp;
	
	public SensornetIncrementalAggregationOperator(LogicalOperator op) throws SNEEException,
			SchemaMetadataException {
		super(op);
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

}
