package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetDeliverOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetDeliverOperator.class.getName());
	
	DeliverOperator delOp;
	
	public SensornetDeliverOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetDeliverOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		delOp = (DeliverOperator) op;		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetDeliverOperator()");
		}		
	}

	
	
}
