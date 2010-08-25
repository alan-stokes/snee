package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetSingleStepAggregationOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetSingleStepAggregationOperator.class.getName());
	
	AggregationOperator aggrOp;
	
	public SensornetSingleStepAggregationOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
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

	
	
}
