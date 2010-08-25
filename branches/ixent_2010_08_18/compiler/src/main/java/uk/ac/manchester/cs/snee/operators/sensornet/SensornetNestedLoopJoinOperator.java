package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetNestedLoopJoinOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetNestedLoopJoinOperator.class.getName());
	
	IStreamOperator delOp;
	
	public SensornetNestedLoopJoinOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
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

	
	
}
