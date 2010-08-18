package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetExchangeOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetExchangeOperator.class.getName());
		
	public SensornetExchangeOperator() 
	throws SNEEException, SchemaMetadataException {
		super(null); ///???
		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER SensornetExchangeOperator() " + op);
//			logger.debug("Attribute List: " + op.getAttributes());
//			logger.debug("Expression List: " + op.getExpressions());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetExchangeOperator()");
		}		
	}

	
	
}
