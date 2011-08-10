package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Stack;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.StringLiteral;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;


public class JoinOperatorImpl extends EvaluationOperator {

	Logger logger = Logger.getLogger(JoinOperatorImpl.class.getName());

	EvaluatorPhysicalOperator leftOperator, rightOperator;
	JoinOperator join;
		
	MultiExpression leftExpr;
	MultiExpression rightExpr;
	CircularArray<Window> leftBuffer,rightBuffer;

	private Expression joinPredicate;

	private List<Attribute> returnAttrs;

	public JoinOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException, EvaluatorException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER JoinOperatorImpl() " + op);
		}

		// Create connections to child operators
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		leftOperator = getEvaluatorOperator(iter.next());
		rightOperator = getEvaluatorOperator(iter.next());
//XXX: Join could be speeded up by working out once which attribute numbers are required from each tuple
		// Instantiate this as a join operator
		join =  (JoinOperator) op;
		int maxBufferSize = SNEEProperties.getIntSetting(
				SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}
		leftBuffer = new CircularArray<Window>(maxBufferSize);
		rightBuffer = new CircularArray<Window>(maxBufferSize);
		joinPredicate = join.getPredicate();
		System.out.println("joinPredicate"+joinPredicate);
		returnAttrs = join.getAttributes();

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN JoinOperatorImpl()");
		}
	}

	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		/*
		 *  Open right child first as it may be a relation!
		 */
		startChildReceiver(rightOperator);
		startChildReceiver(leftOperator);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	private void startChildReceiver(EvaluatorPhysicalOperator op) 
	throws EvaluatorException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER startChildReceiver() " + 
					op.toString());
		}
		op.setSchema(getSchema());
		op.addObserver(this);
		op.open();
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN startChildReceiver()");
		}
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
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() for query " + m_qid + " " +
					" with " + observed + " (" + observed.getClass() + ")");
		}
		try {
			List<Output> resultItems = new ArrayList<Output>();
			if (logger.isTraceEnabled()) {
				logger.trace("Observable: " + obj.toString());
			}
			if (obj == leftOperator) {
				if (logger.isTraceEnabled()) {
					logger.trace("Adding left");
				}
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, 
							leftBuffer);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, 
									resultItems, leftBuffer);
						} else if (output instanceof TaggedTuple) {
							processTuple((TaggedTuple) output, resultItems);
						}
					}
				} else if (observed instanceof TaggedTuple) {
					processTuple((TaggedTuple) observed, resultItems);
				}
			} else if (obj == rightOperator) {
				if (logger.isTraceEnabled()) {
					logger.trace("Adding right");
				}
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, 
							rightBuffer);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, 
									resultItems, rightBuffer);
						}
					}
				} //No checks for tagged tuple on right child as relation always on right!
			} else {
				logger.warn("Notification received from " +
						"unknown source");
			}
			if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}
		} catch (SNEEException e) {
			logger.warn("Error processing join.", e);
//			throw e;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}

	private void processTuple(TaggedTuple taggedTuple, List<Output> resultItems) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processTuple() " + taggedTuple);
		}
		Window relation = 
			(Window) rightBuffer.get(rightBuffer.size()-1);
		Tuple tuple = taggedTuple.getTuple();
		for (Tuple relationTuple : relation.getTuples()) {
			if (evaluate(joinPredicate, tuple, relationTuple)) {
				Tuple joinTuple = 
					generateJoinTuple(tuple, relationTuple);
				resultItems.add(new TaggedTuple(joinTuple));
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processTuple() #resultItems=" +
					resultItems.size());
		}
	}

	private void processWindow(Window window, 
			List<Output> resultItems, CircularArray<Window> buffer)
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWindow() " + window);
		}
		buffer.add(window);
		if (logger.isTraceEnabled()) {
			logger.trace("#leftWindows=" + leftBuffer.size() +
					" #rightWindows=" + rightBuffer.size());
		}
		/* 
		 * Can only compute a join once both buffers have received 
		 * some data!
		 */
		if (leftBuffer.size() > 0 && rightBuffer.size() > 0) {
			resultItems.add(computeJoin());
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWindow() #resultItems=" + 
					resultItems.size());
		}
	}

	private Window computeJoin() throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeJoin()");
		}
		//FIXME: Joins last seen left window with last seen right window. Not really the correct semantics
		Window leftWindow = 
			(Window) leftBuffer.get(leftBuffer.size()-1);
		Window rightWindow = 
			(Window) rightBuffer.get(rightBuffer.size()-1);
		if (logger.isTraceEnabled()) {
			logger.trace("Joining " + leftWindow + " with " + 
					rightWindow);
		}
		System.out.println("joinPredicate: "+ joinPredicate);
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
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeJoin() with " + joinWindow);
		}
		return joinWindow;
	}

	private Tuple generateJoinTuple(Tuple tuple1, Tuple tuple2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER generateJoinTuple() with \n" +
					"[" + tuple1 + 
					"]\n [" + tuple2 + "]");
		}
		System.out.println("Tuple1: "+tuple1);
		System.out.println("Tuple2: "+tuple2);
		Tuple tuple = new Tuple();						
		for (Attribute attr: returnAttrs) {
			String attrName = attr.getAttributeDisplayName();
			if (attrName.equalsIgnoreCase("evalTime") || 
					attrName.equalsIgnoreCase("id") ||
					attrName.equalsIgnoreCase("time")) {
				if (logger.isTraceEnabled()) {
					logger.trace("Ignoring in-network SNEE " +
							"attribute " + attrName);
				}
				continue;
			}
			EvaluatorAttribute evalAttr = 
				retrieveEvalutatorAttribute(tuple1, tuple2, attr);
			if (logger.isTraceEnabled()) {
				logger.trace("Adding attribute: " + evalAttr);
			}
			tuple.addAttribute(evalAttr);
		}
		
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN generateJoinTuple() with join tuple " + 
					tuple);
		}
		System.out.println("Tuple: "+tuple);
		return tuple;
	}

	private EvaluatorAttribute retrieveEvalutatorAttribute(
			Tuple tuple1,
			Tuple tuple2, 
			Attribute attr)
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveEvaluatorAttribute() with \n"+
					tuple1 + ", \n" + tuple2 + ", \n" + attr);
		}
		EvaluatorAttribute evalAttr;
		String extentName = attr.getExtentName();
		try { 
			evalAttr = tuple1.getAttribute(extentName, 
					attr.getAttributeSchemaName());
		} catch (SNEEException e) {
			try {
				evalAttr = tuple2.getAttribute(extentName,
						attr.getAttributeSchemaName());
			} catch (Exception e1) {
				String message = "Unknown attribute " + attr + ".";
				logger.warn(message);
				throw new SNEEException(message);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN retrieveEvaluatorAttribute() with " +
					evalAttr);
		}
		return evalAttr;
	}

	private boolean compute(Expression[] arrExpr, MultiType type, 
			Tuple t1, Tuple t2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER compute() with " + arrExpr +
					" multiType: " + type + "\n" + t1 + "\n" + t2);
		}
		Stack<Object> operands = new Stack<Object>();
		for (int i=0; i < arrExpr.length;i++){
			System.out.println("arrExpr[i]: "+ arrExpr[i]);
			if (arrExpr[i] instanceof DataAttribute){
				DataAttribute da = (DataAttribute) arrExpr[i];
				EvaluatorAttribute evalAttr = 
					retrieveEvalutatorAttribute(t1, t2, da);
				Object daValue = evalAttr.getData();
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push: " + daValue);
				}
				/* 
				 * Check if the value is null 
				 * Nulls are not considered in a join
				 */
				if (daValue instanceof java.sql.Types &&
						(Integer)daValue == java.sql.Types.NULL) {
					logger.warn("Join value is null. Ignore");
					return false;
				}
				operands.add(daValue);
			} else if (arrExpr[i] instanceof IntLiteral){
				IntLiteral il = (IntLiteral) arrExpr[i];
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push integer: " + 
							il.getValue());
				}
				operands.add(new Integer(il.getValue()));
			} else if (arrExpr[i] instanceof FloatLiteral){
				FloatLiteral fl = (FloatLiteral) arrExpr[i];
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push float: " + 
							fl.getValue());
				}
				operands.add(new Float(fl.getValue()));
			} else if (arrExpr[i] instanceof StringLiteral) {
				StringLiteral sl = (StringLiteral) arrExpr[i];
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push float: " + 
							sl.getValue());
				}
				operands.add(sl.getValue());
			}
		}
		boolean retVal = false;
		while (operands.size() >= 2){
			Object op1 = operands.pop();
			System.out.println("Operand 1: "+op1);
			Object op2 = operands.pop();
			System.out.println("Operand 2:"+op2);
			Object result;
			if (op1 instanceof StringLiteral && op2 instanceof StringLiteral) {
				result = evaluateString((String)op1, (String)op2, type);
			} else {
				result = evaluateNumeric((Number)op1, (Number)op2, type);
			}
			if (type.isBooleanDataType()){
				retVal = ((Boolean)result).booleanValue();
			} 
			System.out.println("result: "+result);
			System.out.println("retVal: "+retVal);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN compute() with " + retVal);
		}
		return retVal;
	}

	private boolean evaluate(Expression expr, Tuple tuple1, 
			Tuple tuple2) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluate() with " + expr + ", [" + 
					tuple1 + "], [" + tuple2 + "]");
		}
		System.out.println("Tuple1: "+tuple1);
		System.out.println("Tuple2: "+tuple2);
		Collection<Boolean> returnValList = new ArrayList<Boolean>(2);
		boolean returnValue = false;
		if (expr instanceof MultiExpression) {
			if (logger.isTraceEnabled()) {
				logger.trace("Process MultiExpression: " + expr);
			}
			MultiExpression multiExpr = (MultiExpression) expr;
			MultiExpression mExpr;
			Expression exprTemp;
			for (int i=0; i < multiExpr.getExpressions().length;i++){
				exprTemp = multiExpr.getExpressions()[i];
				if (exprTemp instanceof MultiExpression) {
					System.out.println("The MultiExpression Evalute Seaction");
					mExpr = (MultiExpression)exprTemp;
					returnValue = evaluate(mExpr, tuple1, tuple2);
				}	
				else {
					System.out.println("The Compute Section");
					returnValue = compute(multiExpr.getExpressions(), 
							multiExpr.getMultiType(), tuple1,tuple2);
				}
				returnValList.add(returnValue);
			}
		} else {
			if (logger.isTraceEnabled()) {
				logger.trace("Process Expression: " + expr);
			}
			System.out.println("expr: "+ expr);
			if (expr.toString().equals("TRUE")) {
				returnValue = true;
			}
			//Do something with non-multitype expression
		}
		System.out.println("returnValue: "+ returnValue);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluate() with " + returnValue);
		}
		boolean tempRetVal = true;
		for (Boolean returnVal: returnValList) {
			tempRetVal = tempRetVal & returnVal;
		}
		System.out.println("Correct returnValue: "+ tempRetVal);
		return tempRetVal;
	}
	
}
