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
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class JoinOperatorAbstractImpl extends EvaluationOperator {
	private Logger logger = Logger.getLogger(JoinOperatorAbstractImpl.class.getName());

	protected EvaluatorPhysicalOperator leftOperator, rightOperator;
	protected JoinOperator join;

	protected MultiExpression leftExpr;
	protected MultiExpression rightExpr;

	protected Expression joinPredicate;

	protected List<Attribute> returnAttrs;
	protected int maxBufferSize;
	protected double leftOperatorRate, rightOperatorRate;
	private boolean isFirstLeftOperand = true, isFirstRightOperand = true;
	private long nextEvalTime;
	private Timer timer;
	private EvaluateTask evaluateTask;

	/**
	 * This abstract constructor would initialise all the needed
	 * variables required for the working of the associated join
	 * operator implementation
	 * 
	 * @param op
	 * @param qid
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws EvaluatorException 
	 */
	public JoinOperatorAbstractImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException, EvaluatorException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER JoinOperatorAbstractImpl() " + op);
		}

		// Create connections to child operators
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		LogicalOperator operator = iter.next();
		leftOperator = getEvaluatorOperator(operator);
		leftOperatorRate = operator.getStreamRate();
		operator = iter.next();
		rightOperator = getEvaluatorOperator(operator);
		rightOperatorRate = operator.getStreamRate();
		
		// XXX: Join could be speeded up by working out once which attribute
		// numbers are required from each tuple
		// Instantiate this as a join operator
		join = (JoinOperator) op;
		maxBufferSize = SNEEProperties
				.getIntSetting(SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}

		joinPredicate = join.getPredicate();
		//System.out.println("joinPredicate" + joinPredicate);
		returnAttrs = join.getAttributes();

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN JoinOperatorAbstractImpl()");
		}
	}

	@Override
	public abstract void update(Observable obj, Object observed);
	
	/**
	 * This method is intended to make the operator work in an iterator
	 * model. If this mode is enabled, the operator would pull the next 
	 * set of data (A window or a tuple) by calling the getNext() method
	 * on the the child operator. Since this is a join operator, it would
	 * call the getNext() method on the left and the right child, perform
	 * join and then again call the getNext() method to get the next data
	 * to operate on.
	 * 
	 * TODO For the current implementation, this mode would be enabled only
	 * if there is a valve operator between the join operator and the child
	 * operator. For the Hash join implementation, there would be only
	 * this method implemented, and not update method as it is defined only
	 * to work in the iterator model as of now. The NLJ can work in both
	 * subscribe and pull mode, as per the valve operator settings.
	 * 
	 * @param resultItems 
	 * 
	 * @return 
	 */
	public abstract void generateAndUpdate(List<Output> resultItems);
	
	@Override
	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		/*
		 * Open right child first as it may be a relation!
		 */
		startChildReceiver(rightOperator);
		startChildReceiver(leftOperator);
		if (logger.isDebugEnabled()) {
			logger.debug("Is Pull Mode operator"+join.isGetDataByPullModeOperator());
		}
		if (join.isGetDataByPullModeOperator()) {
			timer = new Timer();
			evaluateTask = new EvaluateTask();
			//long currentTime = System.currentTimeMillis();
			nextEvalTime = getNextEvalTime();
			//TODO Rescheduling of timer needs to be probed into
			//timer.sc
			timer.schedule(evaluateTask, 0, nextEvalTime);
			//timer.
			//evaluateJoinForIntervals();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	/**
	 * This method checks for the rates of the input operands and evoke
	 * the generateAndUpdate method in a timely manner. The time to evoke
	 * is determined by the source rates of both inputs
	 */
	/*private void evaluateJoinForIntervals() {
		if (logger.isDebugEnabled()) {
			logger.debug("Enter evaluateJoinForIntervals()");
		}
		long currentTime = System.currentTimeMillis();
		long nextEvalTime = getNextEvalTime(currentTime);
		
		do {
			currentTime = System.currentTimeMillis();
			
			if (nextEvalTime <= currentTime) {
				generateAndUpdate();
				nextEvalTime = getNextEvalTime(currentTime);
			}
		} while (true);
	}*/

	/**
	 * 
	 */
	private long getNextEvalTime() {
		/*if ((currentTime + leftOperatorRate*1000) < (currentTime + rightOperatorRate*1000)) {
			return (long) (currentTime + leftOperatorRate*1000);
		} else {
			return (long) (currentTime + rightOperatorRate*1000);
		}*/
		//FIXME: Fix for unknown source arrival rates. Probably set the
		//source rate as -1 during setup of the sources, and fix the left
		//or the right operator source rate as 1. This is a crude method
		//and need to be based on some experimentation as how much can the
		//operator process without being overwhelmed.
		long retEvalTime = 1;
		double evalRate = 1.0;
		if (leftOperatorRate == 0 || rightOperatorRate == 0) {
			evalRate = (leftOperatorRate == 0)? rightOperatorRate: leftOperatorRate;
			evalRate = (evalRate == 0)? 1.0:evalRate;			
		} else  if (leftOperatorRate > rightOperatorRate) {
			evalRate = leftOperatorRate;
		} else {
			evalRate = rightOperatorRate;
		}
		retEvalTime = (long)((1/evalRate)*1000);
		return retEvalTime;
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

	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		//super.close();
		timer.cancel();
		timer.purge();		
		timer = null;
		leftOperator.close();
		rightOperator.close();		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}
	
	protected Output getNextFromChild(EvaluatorPhysicalOperator operator) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getNextFromChild()");
		}
		double sourceRate = 0;
		boolean isLeftOperator = false;
		if (operator == leftOperator) {
			sourceRate = leftOperatorRate;
			isLeftOperator = true;
		} else {
			sourceRate = rightOperatorRate;
			isLeftOperator = false;
		}
		if (sourceRate == 0) {
			if (isLeftOperator) {
				if (!isFirstLeftOperand) {
					return null;
				}				
			} else {
				if (!isFirstRightOperand) {
					return null;
				}				
			}
		}
		Output output = operator.getNext();		
		if (output != null) {
			if (isLeftOperator) {
				isFirstLeftOperand = false;
			} else {
				isFirstRightOperand = false;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getNextFromChild() with " + output + 
					" isLeftOperator=" + isLeftOperator);
		}
		return output;
	}
	
	/**
	 * This class runs the Timer for the running the join operation
	 * at regular intervals
	 * 
	 * @author Praveen
	 * 
	 */
	private class EvaluateTask extends TimerTask {

		@Override
		public void run() {
			//System.out.println("Evaluate Task");
			List<Output> resultItems = new ArrayList<Output>(1);
			generateAndUpdate(resultItems);
			//long currentTime = System.currentTimeMillis();
			//nextEvalTime = getNextEvalTime(currentTime);
			//timer.cancel();
			//evaluateTask.cancel();		
			//evaluateTask = null;
			//evaluateTask = new EvaluateTask();
			//timer.schedule(evaluateTask, 0, nextEvalTime);
			if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}
		}

	}

}
