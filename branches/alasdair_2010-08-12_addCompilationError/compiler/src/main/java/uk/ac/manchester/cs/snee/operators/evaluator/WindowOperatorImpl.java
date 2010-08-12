package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.Operator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public abstract class WindowOperatorImpl extends EvaluatorPhysicalOperator {
	//XXX test for window operator

	Logger logger = Logger.getLogger(this.getClass().getName());

	//XXX: This used to be in the helper class, may need to move it back if needed by more than this operator
	protected final static int MAX_BUFFER_SIZE = 20000; 

	protected int windowStart, windowEnd;
	protected WindowOperator windowOp ;

	// Defines the size of slide. Instantiated in constructor
	protected int slide;
	
	public WindowOperatorImpl(Operator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER WindowOperatorImpl() with " + op);
		}

		// Instantiate the window operator
		windowOp = (WindowOperator) op;
		windowStart = windowOp.getFrom();
		windowEnd = windowOp.getTo();
		if (logger.isTraceEnabled()) 
			logger.trace("Window start: " + windowStart + ", Window end: " + windowEnd);

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN WindowOperatorImpl()");
		}
	}
	
//	public abstract Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException;
	
	public abstract void update(Observable obj, Object observed);
	
	/**
	 * @return True if from and to are expressed in ticks.
	 */
	public abstract boolean isTimeScope();
	
}
