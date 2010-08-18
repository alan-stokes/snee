package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;

public class SensornetSelectOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetSelectOperator.class.getName());
	
	SelectOperator selOp;
	
	public SensornetSelectOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetSelectOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		selOp = (SelectOperator) op;		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetSelectOperator()");
		}		
	}

	
	
}
