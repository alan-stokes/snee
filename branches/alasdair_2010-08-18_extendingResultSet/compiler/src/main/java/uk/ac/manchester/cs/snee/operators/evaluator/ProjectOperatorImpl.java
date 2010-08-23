package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Stack;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.operators.logical.Operator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;

public class ProjectOperatorImpl extends EvaluationOperator {
	//XXX: Write test for project operator
	//FIXME: Testing not working

	//FIXME: Add constants as outputs

	/*
	 * Testing of this class is not having the desired effect.
	 * The test does not fail even though the project is not doing its
	 * job 	
	 */
	Logger logger = Logger.getLogger(this.getClass().getName());

	ProjectOperator project;
	List<Attribute> attributes;

	public ProjectOperatorImpl(Operator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ProjectOperatorImpl() " + op.getText());
			logger.debug("Project List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}

		// Instantiate this as a project operator
		project = (ProjectOperator) op;
		attributes = project.getAttributes();

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN ProjectOperatorImpl()");
		}
	}

	/**
	 * Constructor for testing purposes only. 
	 * Stops the recursive nature of the constructor
	 * @param op
	 */
	public ProjectOperatorImpl(ProjectOperator op)
	{
		logger.debug("ENTER ProjectOperatorImpl()");
		project = op;
		attributes = project.getAttributes();
		logger.debug("RETURN ProjectOperatorImpl()");
	}

	//	@Override
	//	public Collection<Output> getNext() 
	//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("ENTER getNext()");
	//		}
	//		// Create bag for result items
	//		Collection<Output> result = new ArrayList<Output>();
	//
	//		// Get the next bag of stream items from the child operator
	//		Collection<Output> bagOfItems = child.getNext();
	//
	//		// Process bag contents
	//		for (Output item : bagOfItems) {
	//			if (item instanceof Window){
	//				Window window = (Window) item;
	//				List<Tuple> tupleList = new ArrayList<Tuple>();
	//				// Process each tuple in the window
	//				for (Tuple t : window.getTuples()){
	//					if (logger.isTraceEnabled()) {
	//						logger.trace("Processing tuple: " + t);
	//					}
	//					Tuple projectedTuple = getProjectedTuple(t);
	//					tupleList.add(projectedTuple);
	//				}
	//				/*
	//				 * Create new window with projected tuples. 
	//				 * The index and evaluation time remain unchanged
	//				 */
	//				//TODO: Do we need a replace tupleList operation?
	//				Window newWindow = new Window(tupleList);
	//				result.add(newWindow);
	//			} else if (item instanceof TaggedTuple) {			
	//				TaggedTuple tuple = (TaggedTuple) item;
	//				if (logger.isTraceEnabled()) {
	//					logger.trace("Processing tuple: " + tuple);
	//				}
	//				Tuple projectedTuple = getProjectedTuple(tuple.getTuple());
	//				// Replace the tuple in the tagged tuple
	//				//XXX What is the meaning of a tick? Is it the time the tuple arrived in the system or the last time it was altered?
	//				tuple.setTuple(projectedTuple);
	//				result.add(tuple);
	//			} else {
	//				String msg = "Unknown item type (" +
	//					item.getClass().getSimpleName() + ") in stream.";
	//				logger.warn(msg);
	//				throw new SNEEException(msg);
	//			}
	//		}
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("RETURN getNext() number of stream items " + result.size());
	//		}
	//		return result;
	//	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() with " + observed);
		List<Output> result = new ArrayList<Output>();
		try {
			if (observed instanceof List<?>) {
				List<Output> outputList = (List<Output>) observed;
				for (Output output : outputList) {
					if (output instanceof Window) {
						result.add(processWindow(output));
					} else if (output instanceof Tuple) {
						result.add(processTuple(output));
					}
				}
			} else if (observed instanceof Window) {
				result.add(processWindow(observed));
			} else if (observed instanceof TaggedTuple) {
				result.add(processTuple(observed));
			}
			setChanged();
			notifyObservers(result);
		} catch (Exception e) {
			logger.error(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}

	private Output processTuple(Object observed)
	throws SNEEException, SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processTuple() with " + observed);
		}
		TaggedTuple tuple = (TaggedTuple) observed;
		if (logger.isTraceEnabled()) {
			logger.trace("Processing tuple: " + tuple);
		}
		Tuple projectedTuple = getProjectedTuple(tuple.getTuple());
		// Replace the tuple in the tagged tuple
		//XXX What is the meaning of a tick? Is it the time the tuple arrived in the system or the last time it was altered?
		tuple.setTuple(projectedTuple);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processTuple() with " + tuple);
		}
		return tuple;
	}

	private Output processWindow(Object observed) 
	throws SNEEException, SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWindow() with " + observed);
		}
		Window window = (Window) observed;
		List<Tuple> tupleList = new ArrayList<Tuple>();
		// Process each tuple in the window
		for (Tuple t : window.getTuples()){
			if (logger.isTraceEnabled()) {
				logger.trace("Processing tuple: " + t);
			}
			Tuple projectedTuple = getProjectedTuple(t);
			tupleList.add(projectedTuple);
		}
		/*
		 * Create new window with projected tuples. 
		 * The index and evaluation time remain unchanged
		 */
		//TODO: Do we need a replace tupleList operation?
		Window newWindow = new Window(tupleList);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWindow() with " + newWindow);
		}
		return newWindow;
	}	

	private Tuple getProjectedTuple(Tuple tuple) 
	throws SNEEException, SchemaMetadataException, TypeMappingException 
	{
		if (logger.isTraceEnabled()) {
			logger.debug("ENTER getProjectedTuple() with " + tuple);
		}		
		Tuple projectedTuple = new Tuple();
		List<Expression> expressions = project.getExpressions();
		logger.trace("Number of expressions: " + expressions.size());
		//Iterate with index so that we can do renames
		for (Expression exp : expressions) {
			Field field = null;
			if (exp instanceof MultiExpression) {
				logger.trace("MultiExpression: " + exp.toString());
				field = evaluateMultiExpression((MultiExpression)exp, tuple);
			} else {
				field = evaluateSingleExpression(exp, tuple);
			}
			// Add project field to the result tuple
			logger.trace("Field: " + field);
			if (field != null) {
				projectedTuple.addField(field);
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getProjectedTuple() with " + projectedTuple);
		}
		return projectedTuple;
	}

	public Field evaluateSingleExpression(Expression exp, Tuple t) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluateSingleExpression() with " + 
					exp.toString() + " and tuple: " + t);
		}
		Field returnField = null;
		// Only a single expression, get attribute details
		List<Attribute> attributes = exp.getRequiredAttributes();
		Attribute attr = attributes.get(0);
		String attributeName = attr.getAttributeName();

		// In-network SNEE adds the following attributes which should just be ignored
		if (attributeName.equalsIgnoreCase("evalTime") ||
				attributeName.equalsIgnoreCase("time") ||
				attributeName.equalsIgnoreCase("id")) {
			logger.trace("Ignoring in-network SNEE assumed attribute " + 
					attributeName);
		} else {
			// Return the field value
			String extentName = attr.getLocalName();
			if (logger.isTraceEnabled()) {
				logger.trace("Keeping attribute " + attributeName +
						" from extent " + extentName);
			}
			if (extentName != null) {
				attributeName = extentName + "." + attributeName;
			}
			try {
				logger.trace("getting attribute " + attributeName);
				returnField = t.getField(attributeName);
			} catch (SNEEException e) {
				logger.warn("Field " + attributeName + 
						" does not exist.", e);
				throw e;
			}
		}					

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluateSingleExpression() with " + 
					returnField);
		}
		return returnField;
	}

	protected Field evaluateMultiExpression(MultiExpression exp, Tuple t) 
	throws SNEEException, SchemaMetadataException, TypeMappingException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluateMultiExpression() with " + 
					exp.toString() + " and tuple: " + t);
		}
		// Instantiate return item
		Field returnField = null;
		String fieldName = "";
		// Setup stack for operands in the expression
		Stack<Object> operands = new Stack<Object>();
		for (Expression expr : exp.getExpressions()) {
			Object daValue;
			if (expr instanceof MultiExpression) {
				Field field = 
					evaluateMultiExpression((MultiExpression)expr, t);
				daValue = field.getData(); 
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push result expression: " + expr + 
							", " + daValue + ", type " + 
							daValue.getClass());
				}
			} else if (expr instanceof DataAttribute){
				DataAttribute da = (DataAttribute) expr;
				String daName = da.getLocalName() + "." + 
					da.getAttributeName();
				try {
					daValue = t.getValue(daName);
				} catch (SNEEException e) {
					logger.warn("Problem getting value for " + daName, e);
					throw e;
				}
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push attribute: " + daName + 
							", " + daValue + ", type " + 
							daValue.getClass());
				}				
			} else if (expr instanceof IntLiteral){
				IntLiteral il = (IntLiteral) expr;
				daValue = new Integer(il.toString());
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push integer: " + 
							il.getMaxValue() + ", type " + 
							daValue.getClass());
				}
			} else if (expr instanceof FloatLiteral){
				FloatLiteral fl = (FloatLiteral) expr;
				daValue = new Float(fl.toString());
				if (logger.isTraceEnabled()) {
					logger.trace("Stack push float: " +
							fl.getMaxValue() + ", type " + 
							daValue.getClass());
				}
			} else {
				logger.warn("Unsupported operand " + expr);
				throw new SNEEException("Unsupported operand " + expr);
			}
			//XXX: Need to think more about the field name 
			fieldName += daValue.toString();
			if (logger.isTraceEnabled())
				logger.trace("Pushing " + daValue + 
						" of type " + daValue.getClass());
			operands.add(daValue);
		}
		while (operands.size() >= 2){
			// Evaluate result
			Object result = evaluate(operands.pop(), operands.pop(), 
					((MultiExpression)exp).getMultiType());
			if (logger.isTraceEnabled())
				logger.trace("Creating new field: " + fieldName + 
						exp.getType() + result);
			returnField = new Field(fieldName, exp.getType(), result);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluateMultiExpression() with " + 
					returnField);
		}
		return returnField;
	}

//	protected Object evaluate(Object obj2, Object obj1, MultiType type){
//		if (logger.isTraceEnabled()) {
//			logger.trace("ENTER evaluate() with " + obj2 + " " + 
//					obj1 + " type " + type);
//		}
//
//		double val1 = ((Number) obj1).doubleValue();
//		double val2 = ((Number) obj2).doubleValue();
//
//		if (logger.isTraceEnabled())
//			logger.trace("Obj1 type " + obj1.getClass() + "\tObj2 type " + obj2.getClass());
//
//		Object returnValue = null;
//		if (type.compareTo(MultiType.EQUALS)== 0){
//			returnValue =  obj1.equals(obj2);	
//		} else if (type.compareTo(MultiType.GREATERTHAN )== 0){
//			returnValue = val1 > val2;
//		} else if (type.compareTo(MultiType.LESSTHAN)== 0){
//			returnValue = val1 < val2;
//		} else if (type.compareTo(MultiType.GREATERTHANEQUALS)== 0){
//			returnValue = val1 >= val2;
//		} else if (type.compareTo(MultiType.LESSTHANEQUALS)== 0){
//			returnValue = val1 <= val2;
//		} else if (type.compareTo(MultiType.NOTEQUALS)== 0){
//			returnValue = val1 != val2;
//		} else if (type.compareTo(MultiType.ADD)== 0){
//			returnValue = val1 + val2;
//		} else if (type.compareTo(MultiType.DIVIDE)== 0){
//			returnValue = val1 / val2;
//		} else if (type.compareTo(MultiType.MULTIPLY)== 0){
//			returnValue = val1 * val2;
//		} else if (type.compareTo(MultiType.MINUS)== 0){
//			returnValue = val1 - val2;
//		} else if (type.compareTo(MultiType.POWER)== 0){
//			returnValue = Math.pow(val1, val2);
//		} else returnValue =  null;
//		if (logger.isTraceEnabled()) {
//			logger.trace("RETURN evaluate() with " + returnValue);
//		}
//		return returnValue;
//	}

}
