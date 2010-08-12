package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.OperatorDataType;

public class SensornetDeliverOperator extends SensornetOperatorImpl {

	
	Logger logger = Logger.getLogger(SensornetDeliverOperator.class.getName());
	
	DeliverOperator deliverOp;
	
	public SensornetDeliverOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetDeliverOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		// Instantiate deliver operator
		deliverOp = (DeliverOperator) op;
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetDeliverOperator()");
		}		
	}

}
