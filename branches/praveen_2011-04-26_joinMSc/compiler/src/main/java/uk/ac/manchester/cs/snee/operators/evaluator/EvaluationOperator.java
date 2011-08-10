package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.Stack;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.StringLiteral;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class EvaluationOperator 
extends EvaluatorPhysicalOperator {
	//TODO: Make all evaluations make use of evaluator class.
	
	Logger logger = Logger.getLogger(this.getClass().getName());
	
	public EvaluationOperator(){
		
	}
	
	public EvaluationOperator(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException, EvaluatorException {
		super(op, qid);
	}
	
	protected EvaluationOperator(EvaluatorPhysicalOperator op) {
		logger.debug("ENTER EvaluationOperator()");
		logger.debug("RETURN EvaluationOperator()");
	}
	
	protected boolean evaluate(MultiExpression expr, Tuple tuple)
	throws SNEEException{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluate() with " + expr + ", " +
					tuple);
		}
		MultiExpression mExpr;
		Expression exprTemp;
	
		// Assume result will be false
		boolean exprResult = false;
	
		for (int i=0; i < expr.getExpressions().length;i++){
			exprTemp = expr.getExpressions()[i];
			if (exprTemp instanceof MultiExpression) {
				mExpr = (MultiExpression)exprTemp;
				exprResult = evaluate(mExpr, tuple);
			}	
			else {
				exprResult = compute(expr.getExpressions(), 
						expr.getMultiType(), tuple);
			}
	
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluate() with " + exprResult);
		}
		return exprResult;
	}

	private boolean compute (Expression[] arrExpr, MultiType type, 
			Tuple tuple) throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER compute() with " + type + ",\n " + 
					tuple);
		}
		// Assume the tuple will not pass the expression
		boolean retVal = false;
		
		// Instantiate stack for evaluation
		Stack<Object> operands = new Stack<Object>();

		// Populate stack with operands
		for (Object operand : arrExpr) {
			Object daValue;
			if (operand instanceof DataAttribute) {
				/* 
				 * Check if the value is null 
				 * Nulls are not considered in a comparison
				 */
				if (operand instanceof java.sql.Types &&
						(Integer)operand == java.sql.Types.NULL) {
					logger.warn("Join value is null. Ignore");
					return false;
				}
				DataAttribute da = (DataAttribute) operand;
				String daExtentName = da.getExtentName();				
				String daAttributeSchemaName = da.getAttributeSchemaName();
				if (logger.isTraceEnabled()) {
					logger.trace("Getting attribute: " +
							daExtentName + ":" + daAttributeSchemaName);
				}
				daValue = 
					tuple.getAttributeValue(daExtentName, daAttributeSchemaName);
			} else if (operand instanceof IntLiteral){
				IntLiteral il = (IntLiteral) operand;
				daValue = new Integer(il.getValue());
			} else if (operand instanceof FloatLiteral){
				FloatLiteral fl = (FloatLiteral) operand;
				daValue = new Float(fl.getValue());
			} else if (operand instanceof StringLiteral) {
				StringLiteral sl = (StringLiteral) operand;
				daValue = sl.getValue();
			} else {
				logger.warn("Unknown operand type " + operand);
				throw new SNEEException("Unknown operand type " +
						operand);
			}
			if (logger.isTraceEnabled()) {
				logger.trace("Stack push: " + daValue + ":" + 
						daValue.getClass());
			}
			operands.add(daValue);
		}

		// Evaluate expression type over operands
		while (operands.size() >= 2){
			Object op1 = operands.pop();
			Object op2 = operands.pop();
			Object result;
			if (logger.isTraceEnabled()) {
				logger.trace("op1: " + op1.getClass() + 
						" op2: " + op2.getClass());
			}
			if (op1 instanceof String && op2 instanceof String) {
//				logger.trace("Two string literals");
				result = evaluateString((String)op1, (String)op2, type);
			} else {
//				logger.trace("Two numeric literals");
				result = evaluateNumeric((Number)op1, (Number)op2, type);
			}
			if (type.isBooleanDataType()){
				retVal = ((Boolean)result).booleanValue();
			} 
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN compute() with " + retVal);
		}
		return retVal;
	}
	
	public Object evaluateNumeric(Number obj2, Number obj1, MultiType type){
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluateNumeric() with " + obj2 + " " + 
					obj1 + " type " + type);
		}
		double val1 = ((Number) obj1).doubleValue();
		double val2 = ((Number) obj2).doubleValue();
		
		Object returnValue = null;
		if (type.compareTo(MultiType.EQUALS)== 0){
			returnValue =  obj1.equals(obj2);	
		} else if (type.compareTo(MultiType.GREATERTHAN )== 0){
			returnValue = val1 > val2;
		} else if (type.compareTo(MultiType.LESSTHAN)== 0){
			returnValue = val1 < val2;
		} else if (type.compareTo(MultiType.GREATERTHANEQUALS)== 0){
			returnValue = val1 >= val2;
		} else if (type.compareTo(MultiType.LESSTHANEQUALS)== 0){
			returnValue = val1 <= val2;
		} else if (type.compareTo(MultiType.NOTEQUALS)== 0){
			returnValue = val1 != val2;
		} else if (type.compareTo(MultiType.ADD)== 0){
			returnValue = val1 + val2;
		} else if (type.compareTo(MultiType.DIVIDE)== 0){
			returnValue = val1 / val2;
		} else if (type.compareTo(MultiType.MULTIPLY)== 0){
			returnValue = val1 * val2;
		} else if (type.compareTo(MultiType.MINUS)== 0){
			returnValue = val1 - val2;
		} else if (type.compareTo(MultiType.POWER)== 0){
			returnValue = Math.pow(val1, val2);
		} else returnValue =  null;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluateNumeric() with " + returnValue);
		}
		return returnValue;
	}
	public boolean evaluateString(String val2, String val1, MultiType type){
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluateString() with " + val2 + " " + 
					val1 + " type " + type);
		}
		
		boolean returnValue = false;
		if (type.compareTo(MultiType.EQUALS)== 0){
			returnValue =  val1.equals(val2);	
//		} else if (type.compareTo(MultiType.GREATERTHAN )== 0){
//			returnValue = val1 > val2;
//		} else if (type.compareTo(MultiType.LESSTHAN)== 0){
//			returnValue = val1 < val2;
//		} else if (type.compareTo(MultiType.GREATERTHANEQUALS)== 0){
//			returnValue = val1 >= val2;
//		} else if (type.compareTo(MultiType.LESSTHANEQUALS)== 0){
//			returnValue = val1 <= val2;
		} else if (type.compareTo(MultiType.NOTEQUALS)== 0){
			returnValue = !(val1.equals(val2));
		} else returnValue =  false;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN evaluateString() with " + returnValue);
		}
		return returnValue;
	}

}
