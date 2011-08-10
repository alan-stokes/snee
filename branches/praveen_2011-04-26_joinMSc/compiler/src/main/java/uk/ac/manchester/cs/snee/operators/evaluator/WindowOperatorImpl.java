package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public abstract class WindowOperatorImpl extends EvaluatorPhysicalOperator {
	//XXX test for window operator

	Logger logger = Logger.getLogger(this.getClass().getName());

	//XXX: This used to be in the helper class, may need to move it back if needed by more than this operator
	protected final static int MAX_BUFFER_SIZE = 20000; 

	protected int windowStart, windowEnd;
	protected WindowOperator windowOp ;
	protected EvaluatorPhysicalOperator sourceOperator;
	protected double sourceOperatorRate;
	private long nextEvalTime;
	private Timer timer;
	private WindowEvaluateTask evaluateTask;

	// Defines the size of slide. Instantiated in constructor
	protected int slide;
	
	public WindowOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException, EvaluatorException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER WindowOperatorImpl() with " + op);
		}

		// Instantiate the window operator
		windowOp = (WindowOperator) op;
		windowStart = windowOp.getFrom();
		windowEnd = windowOp.getTo();
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		LogicalOperator operator = iter.next();		
		sourceOperator = getEvaluatorOperator(operator);
		sourceOperatorRate = operator.getSourceRate();
		if (logger.isTraceEnabled()) 
			logger.trace("Window start: " + windowStart + ", Window end: " + windowEnd);

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN WindowOperatorImpl()");
		}
	}
	
	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		/*
		 * Open right child first as it may be a relation!
		 */
		startChildReceiver(sourceOperator);
		if (windowOp.isGetDataByPullModeOperator()) {
			timer = new Timer();
			evaluateTask = new WindowEvaluateTask();			
			nextEvalTime = getNextEvalTime(sourceOperatorRate);
			timer.schedule(evaluateTask, 0, nextEvalTime);			
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}
	
	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		sourceOperator.close();
		timer.cancel();
		timer.purge();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}
	
	private void startChildReceiver(EvaluatorPhysicalOperator op)
			throws EvaluatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER startChildReceiver() " + op.toString());
		}
		op.setSchema(getSchema());
		op.addObserver(this);
		op.open();
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN startChildReceiver()");
		}
	}
	
	private long getNextEvalTime(double operatorRate) {		
		return (long)((1/operatorRate)*1000);
		
	}
//	public abstract Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException;
	
	public abstract void update(Observable obj, Object observed);
	
	/**
	 * @return True if from and to are expressed in ticks.
	 */
	public abstract boolean isTimeScope();
	
	public abstract void generateAndUpdate(List<Output> resultItems);	
	
	protected Output getNewestEntryofBuffer() {		
		return sourceOperator.getNewestEntry();
	}
	
	/**
	 * This class runs the Timer for the running the 'window'ing operation
	 * at regular intervals
	 * 
	 * @author Praveen
	 * 
	 */
	private class WindowEvaluateTask extends TimerTask {

		@Override
		public void run() {			
			List<Output> resultItems = new ArrayList<Output>(1);
			generateAndUpdate(resultItems);		
			if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}
		}

	}
	
}
