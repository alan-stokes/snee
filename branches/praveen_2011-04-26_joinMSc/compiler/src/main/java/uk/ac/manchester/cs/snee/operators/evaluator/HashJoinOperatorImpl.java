package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
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

public class HashJoinOperatorImpl extends JoinOperatorAbstractImpl {
	Logger logger = Logger.getLogger(HashJoinOperatorImpl.class.getName());

	private Hashtable<Integer, List<Tuple>> operandHashTable;
	private EvaluatorPhysicalOperator hashableOperator, otherOperator;

	public HashJoinOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER HashJoinOperatorImpl() " + op);
		}		
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}
		
		//TODO change the leftoperator to decide on which operator can
		//be hashed based on an algorithm
		if (join.getAlgorithm().equals(JoinOperator.LHJ_MODE)) {
			hashableOperator = leftOperator;
			otherOperator = rightOperator;
		} else {
			hashableOperator = rightOperator;
			otherOperator = leftOperator;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN HashJoinOperatorImpl()");
		}
	}	

	//TODO Implement the following selection criteria in the algorithm
	//selection stage
	/**
	 *
	 * This mode of subscribe to the data published from the child
	 * below is not implemented for the Simple Hash join algorithm
	 * and hence is left blank. So in case there is no valve operator
	 * for the join operator to pull the data from, this algorithm
	 * should not be used. This decision is to be made in the 
	 * Algorithm Selection stage
	 */
	@Override
	public void update(Observable obj, Object observed) {
		/*if (logger.isDebugEnabled()) {
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
		}*/
	}
	/**
	 * Simple Hash Join Algorithm
		1. Get left operand
		2. If the left operand is not null, then
			a. Clear the left hash table
			b. Build the left hash table from the left operand
		3. Get the right operand
		4. If the right operand is not null, then
			i. Generate the hash value for the right operand
			ii. perform join of right hash value over the left hash table
		6. Goto Step 1
	 *

	 */
	@Override
	public void generateAndUpdate(List<Output> resultItems) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER generateAndUpdate() for query " + m_qid);
		}		
		
		//List<Output> resultItems = new ArrayList<Output>(1);

		Output operand;
		try {
			operand = getNextFromChild(hashableOperator);
			if (logger.isDebugEnabled()) {
				logger.debug("generateAndUpdate() Got the first operand" + operand);
			}
			
			if (operand != null) {
				//System.out.println("Hashable operand"+operand);
				if (operandHashTable != null && operandHashTable.size() > 0) {
					operandHashTable.clear();
				} 
				operandHashTable = buildHashTable(operand);
			}
			if (operandHashTable != null && operandHashTable.size() > 0) {
				operand = getNextFromChild(otherOperator);
				//System.out.println("Other operand"+operand);
				if (logger.isDebugEnabled()) {
					logger.debug("generateAndUpdate() Got the second operand" + operand);
				}
				if (operand != null) {
					computeJoin(operandHashTable, operand, resultItems);
				}
			}

			/*if (!resultItems.isEmpty()) {
				setChanged();
				notifyObservers(resultItems);
			}*/
		} catch (SNEEException sneeException) {
			logger.warn("Error processing join.", sneeException);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Exit generateAndUpdate()");
		}
	}	
	

	/**
	 * This method computes the join and fills the result items collection
	 * with the generated tuples
	 * 
	 * @param hashTable
	 * @param output
	 * @param resultItems
	 * @throws SNEEException
	 */
	private void computeJoin(Hashtable<Integer, List<Tuple>> hashTable,
			Output output, List<Output> resultItems) throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("Enter computeJoin()");
		}
		if (output instanceof Window)  {
			Window joinTuple = processWindowJoin(hashTable, (Window)output);
			if (joinTuple != null) {
				resultItems.add(joinTuple);
			}
		} else if (output instanceof TaggedTuple) {
			processTaggedTupleJoin(hashTable, (TaggedTuple)output, resultItems);			
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Exit computeJoin()");
		}
		
	}

	private void processTaggedTupleJoin(
			Hashtable<Integer, List<Tuple>> hashTable, TaggedTuple taggedTuple,
			List<Output> resultItems) throws SNEEException {

		Tuple tuple = taggedTuple.getTuple();
		int hashKey = generateHashKey(joinPredicate, tuple);
		List<Tuple> hashedTuples = hashTable.get(hashKey);
		if (hashedTuples != null && hashedTuples.size() > 0) {
			for (Tuple innerTuple: hashedTuples) {
				Tuple joinTuple = generateJoinTuple(innerTuple, tuple);
				System.out.println("joinTuple: "+ joinTuple);				
				resultItems.add(new TaggedTuple(joinTuple));
			}
		}
	}

	private Window processWindowJoin(Hashtable<Integer, List<Tuple>> hashTable,
			Window window) throws SNEEException {
		List<Tuple> joinTuples = null;
		for (Tuple outerTuple: window.getTuples()) {
			System.out.println("Outer Tuple: " +outerTuple);
			int hashKey = generateHashKey(joinPredicate, outerTuple);
			System.out.println("Hash Key for outer tuple: "+hashKey);
			outputHashTable(hashTable);
			System.out.println("Do "+hashTable.keySet()+ " contain "+hashKey + "?");
			List<Tuple> hashedTuples = hashTable.get(hashKey); 
			if (hashedTuples != null && hashedTuples.size() > 0) {
				for (Tuple innerTuple: hashedTuples) {
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
		if (joinTuples != null) {
			joinWindow = new Window(joinTuples);
		}
		return joinWindow;
		
	}

	/**
	 * A hash join is implemented, where in at least one of the operands
	 * can be fully held in memory. Even though this concept does not hold
	 * in case of streams, a modified version of it is assumed here.
	 * 
	 * So if there is a join between 
	 * stream & relation - relation is hashed
	 * stream & window - window is hashed
	 * window & window - one of the windows is hashed
	 * 
	 * The above selection is performed in the Algorithm selection phase,
	 * and the join operator is set to be of LHJ or RHJ according to this.
	 * This means a stream of tagged tuples is never used to build the 
	 * hash table and hence the otherOperand can be tagged tuples or windows,
	 * but the hashable operand is never a stream of tagged tuples
	 *  
	 * @param hashableOperand
	 * @return
	 * @throws SNEEException
	 */
	private Hashtable<Integer, List<Tuple>> buildHashTable(
			Output hashableOperand) throws SNEEException {
		Hashtable<Integer, List<Tuple>> hashTable = null;
		if (hashableOperand instanceof Window) {
			hashTable = generateHashTableForWindow((Window) hashableOperand);
		} else if (hashableOperand instanceof TaggedTuple) {
			System.out.println("Never supposed to be here");
			//This is never supposed to happen
			hashTable = generateHashTableForTaggedTuple((TaggedTuple) hashableOperand);
		}
		return hashTable;
	}

	private Hashtable<Integer, List<Tuple>> generateHashTableForTaggedTuple(
			TaggedTuple hashableOperand) {
		Hashtable<Integer, List<Tuple>> hashTable = null;
		
		return hashTable;
	}

	/**
	 * Generate the hash table for the window of tuples
	 * 
	 * @param window
	 * @return
	 * @throws SNEEException
	 */
	private Hashtable<Integer, List<Tuple>> generateHashTableForWindow(
			Window window) throws SNEEException {
		Hashtable<Integer, List<Tuple>> hashTable = null;
		for (Tuple innerTuple : window.getTuples()) {
			System.out.println("innerTuple"+innerTuple);
			int hashKey = generateHashKey(joinPredicate, innerTuple);
			System.out.println("hashKey: "+ hashKey);
			// getHashKey(leftTuple, joinPredicate);
			if (hashTable == null) {
				hashTable = new Hashtable<Integer, List<Tuple>>(maxBufferSize);
			}
			if (!hashTable.containsKey(hashKey)) {
				hashTable.put(hashKey, new ArrayList<Tuple>(1));
			}
			hashTable.get(hashKey).add(innerTuple);
			System.out.println("Parent: ");					
		}
		if (hashTable != null) {
			outputHashTable(hashTable);
		}
		return hashTable;
	}

	/**
	 * For a hash join operator the child operator is assumed to
	 * be a valve operator. Rather a valve is considered to be the buffer
	 * for the join and is also a part of the join operation, but maintained 
	 * separately
	 * 
	 * @param operator
	 * @return
	 */
	private Output getNextFromChild(EvaluatorPhysicalOperator operator) {
		return ((ValveOperatorAbstractImpl)operator).getNext();
	}

	
	
	private void outputHashTable (Hashtable<Integer, List<Tuple>> currOperatorHT) {
		System.out.println("Printing out the hash table");
		for (Map.Entry<Integer, List<Tuple>> curEntry: currOperatorHT.entrySet()) {
			System.out.println("Key: "+curEntry.getKey());
			for (Tuple tuple: curEntry.getValue()) {
				System.out.println(tuple.toString());
			}
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
