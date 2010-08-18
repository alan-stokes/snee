package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAcquireOperator extends SensornetOperatorImpl {

	private static Logger logger 
	= Logger.getLogger(SensornetAcquireOperator.class.getName());
	
	AcquireOperator acqOp;
	
	public SensornetAcquireOperator(LogicalOperator op) throws SNEEException,
			SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAcquireOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		acqOp = (AcquireOperator) op;		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAcquireOperator()");
		}		
	}

}
