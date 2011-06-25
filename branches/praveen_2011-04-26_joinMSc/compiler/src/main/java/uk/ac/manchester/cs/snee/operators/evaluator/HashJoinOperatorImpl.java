package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;

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
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class HashJoinOperatorImpl extends EvaluationOperator {
	Logger logger = Logger.getLogger(HashJoinOperatorImpl.class.getName());

	private EvaluatorPhysicalOperator leftOperator, rightOperator;
	private JoinOperator join;
	
	private CircularArray<Window> leftBuffer, rightBuffer;
	private Hashtable<Integer, List<Tuple>> leftHashTable, rightHashTable;

	private Expression joinPredicate;

	private List<Attribute> returnAttrs;

	public HashJoinOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER HashJoinOperatorImpl() " + op);
		}

		// Create connections to child operators
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		leftOperator = getEvaluatorOperator(iter.next());
		rightOperator = getEvaluatorOperator(iter.next());
		// XXX: Join could be speeded up by working out once which attribute
		// numbers are required from each tuple
		// Instantiate this as a join operator
		join = (JoinOperator) op;
		int maxBufferSize = SNEEProperties
				.getIntSetting(SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}
		leftBuffer = new CircularArray<Window>(maxBufferSize);
		rightBuffer = new CircularArray<Window>(maxBufferSize);
		leftHashTable = new Hashtable<Integer, List<Tuple>>(maxBufferSize);
		rightHashTable = new Hashtable<Integer, List<Tuple>>(maxBufferSize);
		joinPredicate = join.getPredicate();
		System.out.println(joinPredicate);
		System.out.println(op);
		/*joinPredicate = getJoinPredicateForSelect(op);
		System.out.println(joinPredicate);
		System.out.println(op);*/
		returnAttrs = join.getAttributes();

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN HashJoinOperatorImpl()");
		}
	}

	/*private Expression getJoinPredicateForSelect(LogicalOperator op) {
		LogicalOperator tempOp = op.getParent();
		while (!(tempOp instanceof SelectOperator)) {
			System.out.println("Looping In here mate"+tempOp);
			tempOp = tempOp.getParent();
		}		
		return tempOp.getPredicate();
	}*/

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
			logger.debug("RETURN open()");
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

	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		leftOperator.close();
		rightOperator.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() for query " + m_qid + " " + " with "
					+ observed + " (" + observed.getClass() + ")");
		}
		try {
			List<Output> resultItems = new ArrayList<Output>();
			if (logger.isTraceEnabled()) {
				logger.trace("Observable: " + obj.toString());
			}
			if (obj == leftOperator) {
				System.out.println("Left HashTable");
				if (logger.isTraceEnabled()) {
					logger.trace("Adding left");
				}
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, leftBuffer,
							leftHashTable, rightHashTable);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, resultItems,
									leftBuffer, leftHashTable, rightHashTable);
						} else if (output instanceof TaggedTuple) {
							processTuple((TaggedTuple) output, resultItems);
						}
					}
				} else if (observed instanceof TaggedTuple) {
					processTuple((TaggedTuple) observed, resultItems);
				}
			} else if (obj == rightOperator) {
				System.out.println("Right HashTable");
				if (logger.isTraceEnabled()) {
					logger.trace("Adding right");
				}
				if (observed instanceof Window) {
					processWindow((Window) observed, resultItems, rightBuffer,
							rightHashTable, leftHashTable);
				} else if (observed instanceof List<?>) {
					List<Output> outputList = (List<Output>) observed;
					for (Output output : outputList) {
						if (output instanceof Window) {
							processWindow((Window) output, resultItems,
									rightBuffer, rightHashTable, leftHashTable);
						}
					}
				} // No checks for tagged tuple on right child as relation
					// always on right!
			} else {
				logger.warn("Notification received from " + "unknown source");
			}
			if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}
		} catch (SNEEException e) {
			logger.warn("Error processing join.", e);
			// throw e;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}

	private void processWindow(Window window, List<Output> resultItems,
			CircularArray<Window> buffer,
			Hashtable<Integer, List<Tuple>> currOperatorHT,
			Hashtable<Integer, List<Tuple>> otherOperatorHT)
			throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWindow() " + window);
		}
		System.out.println("Process Window");
		buffer.add(window);
		Window joinTuple = computeJoin(buffer, currOperatorHT, otherOperatorHT);
		if (joinTuple != null) {
			resultItems.add(joinTuple);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWindow() #resultItems="
					+ resultItems.size());
		}

	}

	private Window computeJoin(CircularArray<Window> buffer,
			Hashtable<Integer, List<Tuple>> currOperatorHT,
			Hashtable<Integer, List<Tuple>> otherOperatorHT)
			throws SNEEException {
		Window window = buffer.get(buffer.size() - 1);
		List<Tuple> joinTuples = null;
		for (Tuple innerTuple : window.getTuples()) {
			System.out.println("innerTuple"+innerTuple);
			int hashKey = generateHashKey(joinPredicate, innerTuple);
			System.out.println("hashKey: "+ hashKey);
			// getHashKey(leftTuple, joinPredicate);
			if (!currOperatorHT.containsKey(hashKey)) {
				currOperatorHT.put(hashKey, new ArrayList<Tuple>(2));
			}
			currOperatorHT.get(hashKey).add(innerTuple);
			System.out.println("Parent: ");
			outputHashTable(currOperatorHT);
			System.out.println("Child: ");
			outputHashTable(otherOperatorHT);
			if (otherOperatorHT.containsKey(hashKey)) {
				for (Tuple outerTuple : otherOperatorHT.get(hashKey)) {
					Tuple joinTuple = generateJoinTuple(innerTuple, outerTuple);
					System.out.println("joinTuple: "+ joinTuple);
					if (joinTuples == null) {
						joinTuples = new ArrayList<Tuple>(2);
					}
					joinTuples.add(joinTuple);
				}
			}
		}
		Window joinWindow = null;
		if (joinTuples != null && joinTuples.size() > 0) {
			joinWindow = new Window(joinTuples);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeJoin() with " + joinWindow);
		}
		return joinWindow;
	}
	
	private void outputHashTable (Hashtable<Integer, List<Tuple>> currOperatorHT) {
		for (Map.Entry<Integer, List<Tuple>> curEntry: currOperatorHT.entrySet()) {
			System.out.println("Key: "+curEntry.getKey());
			for (Tuple tuple: curEntry.getValue()) {
				System.out.println(tuple.toString());
			}
		}
	}

	private void processTuple(TaggedTuple taggedTuple, List<Output> resultItems)
			throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processTuple() " + taggedTuple);
		}
		Window relation = (Window) rightBuffer.get(rightBuffer.size() - 1);
		Tuple tuple = taggedTuple.getTuple();
		int leftHashKey = generateHashKey(joinPredicate, tuple);
		if (!leftHashTable.containsKey(leftHashKey)) {
			leftHashTable.put(leftHashKey, new ArrayList<Tuple>(2));
		}
		leftHashTable.get(leftHashKey).add(tuple);
		for (Tuple relationTuple : relation.getTuples()) {
			int rightHashKey = generateHashKey(joinPredicate, relationTuple);
			// getHashKey(leftTuple, joinPredicate);
			if (!leftHashTable.containsKey(rightHashKey)) {
				rightHashTable.put(rightHashKey, new ArrayList<Tuple>(2));
			}
			rightHashTable.get(rightHashKey).add(relationTuple);

		}
		if (rightHashTable.containsKey(leftHashKey)) {
			for (Tuple outerTuple : rightHashTable.get(leftHashKey)) {
				Tuple joinTuple = generateJoinTuple(tuple, outerTuple);
				resultItems.add(new TaggedTuple(joinTuple));
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processTuple() #resultItems="
					+ resultItems.size());
		}
	}
	

	private Tuple generateJoinTuple(Tuple tuple1, Tuple tuple2)
			throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER generateJoinTuple() with \n" + "[" + tuple1
					+ "]\n [" + tuple2 + "]");
		}
		Tuple tuple = new Tuple();
		System.out.println("generateJoinTuple: ");
		for (Attribute attr : returnAttrs) {
			String attrName = attr.getAttributeDisplayName();
			if (attrName.equalsIgnoreCase("evalTime")
					|| attrName.equalsIgnoreCase("id")
					|| attrName.equalsIgnoreCase("time")) {
				if (logger.isTraceEnabled()) {
					logger.trace("Ignoring in-network SNEE " + "attribute "
							+ attrName);
				}
				continue;
			}
			EvaluatorAttribute evalAttr = retrieveEvalutatorAttribute(tuple1,
					tuple2, attr);
			if (logger.isTraceEnabled()) {
				logger.trace("Adding attribute: " + evalAttr);
			}
			tuple.addAttribute(evalAttr);
		}
		System.out.println("tuple: "+tuple);

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN generateJoinTuple() with join tuple " + tuple);
		}
		return tuple;
	}

	private EvaluatorAttribute retrieveEvalutatorAttribute(Tuple tuple1,
			Tuple tuple2, Attribute attr) throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveEvaluatorAttribute() with \n" + tuple1
					+ ", \n" + tuple2 + ", \n" + attr);
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
			logger.trace("RETURN retrieveEvaluatorAttribute() with " + evalAttr);
		}
		return evalAttr;
	}
	

	/**
	 * Generates the hash key by looping through the data 
	 * attributes specified in the predicate expressions.
	 * 
	 * @param expr
	 * @param tuple
	 * @return
	 * @throws SNEEException
	 */
	private int generateHashKey(Expression expr, Tuple tuple)
			throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluate() with " + expr + ", [" + tuple + "]");
		}
		System.out.println("generateHashKey: ");
		int returnValue = 1;
		if (expr instanceof MultiExpression) {
			if (logger.isTraceEnabled()) {
				logger.trace("Process MultiExpression: " + expr);
			}
			System.out.println("expr: "+expr);
			MultiExpression multiExpr = (MultiExpression) expr;
			MultiExpression mExpr;
			Expression exprTemp;
			for (int i = 0; i < multiExpr.getExpressions().length; i++) {
				exprTemp = multiExpr.getExpressions()[i];
				if (exprTemp instanceof MultiExpression) {
					mExpr = (MultiExpression) exprTemp;
					returnValue = generateHashKey(mExpr, tuple);
				} else {
					returnValue = calculateHash(multiExpr.getExpressions(),
							tuple);
				}
			}
		} else {
			if (logger.isTraceEnabled()) {
				logger.trace("Process Expression: " + expr);
			}
			if (expr.toString().equals("TRUE")) {
				returnValue = 1;
			}
			// Do something with non-multitype expression
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluate() with " + returnValue);
		}
		System.out.println("returnValue: "+returnValue);
		return returnValue;
	}

	/**
	 * calculating the hash value by iterating through the data attributes
	 * 
	 * @param expr
	 * @param tuple
	 * @return
	 * @throws SNEEException
	 */
	private int calculateHash(Expression[] expr, Tuple tuple)
			throws SNEEException {
		int hashCode = 1;
		for (int i = 0; i < expr.length; i++) {
			System.out.println("expr[i]: "+expr[i]);
			if (expr[i] instanceof DataAttribute) {
				DataAttribute da = (DataAttribute) expr[i];
				EvaluatorAttribute evalAttr;
				try {
					evalAttr = retrieveEvalutatorAttribute(
							tuple, da);
				} catch (SNEEException sneeException) {
					/*if (i +1 == expr.length) {
						System.out.println("i: "+i+ " expr.length"+expr.length);
						System.out.println("Exception man: "+sneeException);
						throw sneeException;
					}*/
					continue;
				}

				hashCode = 31 * hashCode
						+ (evalAttr == null ? 0 : evalAttr.hashCode());
			}
		}
		return hashCode;
	}

	private EvaluatorAttribute retrieveEvalutatorAttribute(Tuple tuple,
			Attribute attribute) throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveEvaluatorAttribute() with \n" + tuple
					+ ", \n" + attribute);
		}
		EvaluatorAttribute evalAttr;
		String extentName = attribute.getExtentName();
		try {
			evalAttr = tuple.getAttribute(extentName,
					attribute.getAttributeSchemaName());
		} catch (SNEEException e) {
			String message = "Unknown attribute " + attribute + ".";
			logger.warn(message);
			throw new SNEEException(message);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN retrieveEvaluatorAttribute() with " + evalAttr);
		}
		return evalAttr;
	}

}
