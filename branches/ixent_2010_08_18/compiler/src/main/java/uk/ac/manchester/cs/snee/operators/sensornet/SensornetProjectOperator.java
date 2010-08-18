package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;

public class SensornetProjectOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetProjectOperator.class.getName());
	
	ProjectOperator prjOp;
	
	public SensornetProjectOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetProjectOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		prjOp = (ProjectOperator) op;		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetProjectOperator()");
		}		
	}

	
	
}
