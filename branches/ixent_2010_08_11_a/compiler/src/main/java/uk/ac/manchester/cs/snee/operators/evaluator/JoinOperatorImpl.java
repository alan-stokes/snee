package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Stack;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.evaluator.types.CircularList;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;


public class JoinOperatorImpl extends EvaluationOperator {

	Logger logger = Logger.getLogger(JoinOperatorImpl.class.getName());

	EvaluatorPhysicalOperator leftOperator, rightOperator;
	JoinOperator join;
		
	MultiExpression leftExpr;
	MultiExpression rightExpr;
	CircularList leftBuffer,rightBuffer;

	private Expression joinPredicate;

	private List<Attribute> attrs;

	public JoinOperatorImpl(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER JoinOperatorImpl() " + op);
		}

		// Create connections to child operators
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		leftOperator = getEvaluatorOperator(iter.next());
		rightOperator = getEvaluatorOperator(iter.next());

		// Instantiate this as a join operator
		join =  (JoinOperator) op;
		leftBuffer = new CircularList();
		rightBuffer = new CircularList();
		joinPredicate = join.getPredicate();
		attrs = join.getAttributes();

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN JoinOperatorImpl()");
		}
	}

	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		startChildReceiver(leftOperator);
		startChildReceiver(rightOperator);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	private void startChildReceiver(EvaluatorPhysicalOperator op) 
	throws EvaluatorException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER startChildReceiver() " + op.toString());
		op.setSchema(getSchema());
		op.open();
		op.addObserver(this);
		if (logger.isTraceEnabled())
			logger.trace("RETURN startChildReceiver()");
	}

	public void close(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		leftOperator.close();
		rightOperator.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

//	@Override
//	public Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER getNext()");
//		}
//		// Instantiate the return collection
//		Collection<Output> resultWindows = new ArrayList<Output>();
//		
//		// Get results from child operators
//		Collection<Output> leftWindows = leftOperator.getNext();
//		Collection<Output> rightWindows = rightOperator.getNext();
//		if (logger.isTraceEnabled()) {
//			logger.trace("Number of windows on left stream: " + leftWindows.size() +
//					"\n\t\t\t\t\t\tNumber of windows on right stream: " + rightWindows.size());
//		}
//		//XXX: What if the number of windows is different from the two child operators?		
//		
//		// Iterate over the bag of windows
//		Iterator<Output> leftIter = leftWindows.iterator();
//		Iterator<Output> rightIter = rightWindows.iterator();
//		for (int i = 0; i < leftWindows.size(); i++) {
//			if (logger.isTraceEnabled()) {
//				logger.trace("Joining window " + i + " in stream.");
//			}
//			List<Tuple> joinTuples = new ArrayList<Tuple>();
//			Window leftWindow = (Window) leftIter.next();
//			Window rightWindow = (Window) rightIter.next();
//			
//			// Iterate over the tuples in the left window
//			for (Tuple t1: leftWindow.getTuples()){
//				// Try joining each tuple in the right window with the tuple from the left
//				for (Tuple t2: rightWindow.getTuples()){
//					boolean valid = evaluate(joinPredicate, t1, t2 );
//					if (valid) {
//						Tuple joinTuple = generateJoinTuple(t1, t2);
//						joinTuples.add(joinTuple);
//					}
//				}
//			}
//			
//			// Create join window
//			Window joinWindow = new Window(joinTuples);
//
//			// Add join window to result
//			resultWindows.add(joinWindow);
//		}
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN getNext() number of join windows " + 
//					resultWindows.size());
//		}
//		return resultWindows;
//	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() with " + observed);
		try {
			List<Output> resultItems = new ArrayList<Output>();
			logger.trace("Observable: " + obj.toString());
			if (obj == leftOperator) {
				logger.trace("Adding left");
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, leftBuffer);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, resultItems, leftBuffer);
						}
					}
				}
			} else if (obj == rightOperator) {
				logger.trace("Adding right");
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, rightBuffer);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, resultItems, rightBuffer);
						}
					}
				}
			} else {
				logger.warn("Notification received from unknown source");
			}
			if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}
		} catch (SNEEException e) {
			logger.warn("Error processing join");
//			throw e;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");		
	}

	private void processWindow(Window window, List<Output> resultItems, 
			CircularList buffer)
	throws SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processWindow()");
		buffer.add(window);
		if (logger.isTraceEnabled())
			logger.trace("#leftWindows=" + leftBuffer.size() +
					" #rightWindows=" + rightBuffer.size());
		//FIXME: Can only compute join once both buffers have received some data!
		if (leftBuffer.size() > 0 && rightBuffer.size() > 0) {
			resultItems.add(computeJoin());
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processWindow() #resultItems=" + 
					resultItems.size());
	}

	private Window computeJoin() throws SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER computeJoin()");
		//FIXME: Joins last seen left window with last seen right window. Not really the correct semantics
		Window leftWindow = (Window) leftBuffer.get(leftBuffer.size()-1);
		Window rightWindow = (Window) rightBuffer.get(rightBuffer.size()-1);
		if (logger.isTraceEnabled())
			logger.trace("Joining " + leftWindow + " with " + rightWindow);
		List<Tuple> joinTuples = new ArrayList<Tuple>();
		for (Tuple leftTuple : leftWindow.getTuples()) {
			for (Tuple rightTuple : rightWindow.getTuples()) {
				if (evaluate(joinPredicate, leftTuple, rightTuple)) {
					Tuple joinTuple = 
						generateJoinTuple(leftTuple, rightTuple);
					joinTuples.add(joinTuple);
				}
			}
		}
		// Create join window
		Window joinWindow = new Window(joinTuples);
		if (logger.isTraceEnabled())
			logger.trace("RETURN computeJoin() with " + joinWindow);
		return joinWindow;
	}

	private Tuple generateJoinTuple(Tuple tuple1, Tuple tuple2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER generateJoinTuple() with [" + tuple1 + 
					"], [" + tuple2 + "]");
		}
		Tuple tuple = new Tuple();						
		for (Attribute attr: attrs) {
			String attrName = attr.getAttributeName();
			if (attrName.equalsIgnoreCase("evalTime") || 
					attrName.equalsIgnoreCase("id") ||
					attrName.equalsIgnoreCase("time")) {
				if (logger.isTraceEnabled()) {
					logger.trace("Ignoring in-network SNEE attribute " + 
							attrName);
				}
				continue;
			}
			String qualAttrName = attr.getLocalName() + "." + attrName;
			if (logger.isTraceEnabled()) {
				logger.trace("Attribute: " + qualAttrName);
			}
			Field field;
			if (tuple1.containsField(qualAttrName)) {
				field = tuple1.getField(qualAttrName);
			} else if (tuple2.containsField(qualAttrName)) {
				field = tuple2.getField(qualAttrName);
			} else {
				logger.warn("Unknown attribute " + qualAttrName + ".");
				throw new SNEEException("Unknown attribute " + qualAttrName + ".");
			}
			field.setName(qualAttrName);
			if (logger.isTraceEnabled()) {
				logger.trace("Adding field: " + field.getName() + " " + field);
			}
			tuple.addField(field);
		}
		
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN generateJoinTuple() with join tuple " + 
					tuple);
		}
		return tuple;
	}

	private boolean compute (Expression[] arrExpr, MultiType type, 
			Tuple t1, Tuple t2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER compute()");
		}
		Stack<Object> operands = new Stack<Object>();
		for (int i=0; i < arrExpr.length;i++){
			if (arrExpr[i] instanceof DataAttribute){
				DataAttribute da = (DataAttribute) arrExpr[i];
				String daName = da.getLocalName() + "." + da.getAttributeName();
				Object daValue;
				if (t1.containsField(daName)) {
					daValue = t1.getValue(daName);
				} else if (t2.containsField(daName)) {
					daValue = t2.getValue(daName);
				} else {
					logger.warn("Unknown attribute name " + daName);
					throw new SNEEException("Unknown attribute name " + daName);
				}
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push: " + daName + ", " + daValue);
				}
				operands.add(daValue);
			} else if (arrExpr[i] instanceof IntLiteral){
				IntLiteral il = (IntLiteral) arrExpr[i];
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push integer: " + il.getMaxValue());
				}
				operands.add(new Integer(il.toString()));
			} else if (arrExpr[i] instanceof FloatLiteral){
				FloatLiteral fl = (FloatLiteral) arrExpr[i];
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push float: " + fl.getMaxValue());
				}
				operands.add(new Float(fl.toString()));
			}
		}
		boolean retVal = false;
		while (operands.size() >= 2){
			Object result = evaluate(operands.pop(), operands.pop(), type);
			if (type.isBooleanDataType()){
				retVal = ((Boolean)result).booleanValue();
			} 
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN compute() with " + retVal);
		}
		return retVal;
	}

	private boolean evaluate(Expression expr, Tuple tuple1, Tuple tuple2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluate() with " + expr + ", [" + 
					tuple1 + "], [" + tuple2 + "]");
		}
		boolean returnValue = false;
		if (expr instanceof MultiExpression) {
			if (logger.isTraceEnabled())
				logger.trace("Process MultiExpression: " + expr);
			MultiExpression multiExpr = (MultiExpression) expr;
			MultiExpression mExpr;
			Expression exprTemp;
			for (int i=0; i < multiExpr.getExpressions().length;i++){
				exprTemp = multiExpr.getExpressions()[i];
				if (exprTemp instanceof MultiExpression) {
					mExpr = (MultiExpression)exprTemp;
					returnValue = evaluate(mExpr, tuple1, tuple2);
				}	
				else {
					returnValue = compute(multiExpr.getExpressions(), multiExpr.getMultiType(), tuple1,tuple2);
				}
			}
		} else {
			if (logger.isTraceEnabled()) {
				logger.trace("Process Expression: " + expr);
			}
			if (expr.toString().equals("TRUE"))
				returnValue = true;
			//Do something with non-multitype expression
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluate() with " + returnValue);
		}
		return returnValue;
	}
	
}
