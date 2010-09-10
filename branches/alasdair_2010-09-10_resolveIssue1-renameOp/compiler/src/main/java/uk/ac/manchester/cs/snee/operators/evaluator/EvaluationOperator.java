package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.Stack;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class EvaluationOperator 
extends EvaluatorPhysicalOperator {
	//TODO: Make all evaluations make use of evaluator class.
	
	Logger logger = Logger.getLogger(this.getClass().getName());
	
	public EvaluationOperator(){
		
	}
	
	public EvaluationOperator(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
	}
	
	protected EvaluationOperator(EvaluatorPhysicalOperator op) {
		logger.debug("ENTER EvaluationOperator()");
		logger.debug("RETURN EvaluationOperator()");
	}
	
//FIXME: Working here!!!
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
			logger.trace("ENTER compute() with " + type + ", " + tuple);
		}
		// Assume the tuple will not pass the expression
		boolean retVal = false;
		
		// Instantiate stack for evaluation
		Stack<Object> operands = new Stack<Object>();

		// Populate stack with operands
		for (Object operand : arrExpr) {
			Object daValue;
			if (operand instanceof DataAttribute) {
				DataAttribute da = (DataAttribute) operand;
				String daName = da.getLocalName() + "." + 
					da.getAttributeName();
				if (logger.isTraceEnabled()) {
					logger.trace("Getting attribute " + daName);
				}
				daValue = tuple.getAttributeValue(daName);
			} else if (operand instanceof IntLiteral){
				IntLiteral il = (IntLiteral) operand;
				daValue = new Integer(il.toString());
			} else if (operand instanceof FloatLiteral){
				FloatLiteral fl = (FloatLiteral) operand;
				daValue = new Float(fl.toString());
			} else {
				logger.warn("Unknown operand type " + operand);
				throw new SNEEException("Unknown operand type " +
						operand);
			}
			if (logger.isTraceEnabled()) {
				logger.trace("Stack push: " + daValue);
			}
			operands.add(daValue);
		}

		// Evaluate expression type over operands
		while (operands.size() >= 2){
			Object result = evaluate(operands.pop(), operands.pop(), 
					type);
			if (type.isBooleanDataType()){
				retVal = ((Boolean)result).booleanValue();
			} 
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN compute() with " + retVal);
		}
		return retVal;
	}
	
	public Object evaluate(Object obj2, Object obj1, MultiType type){
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER evaluate() with " + obj2 + " " + 
					obj1 + " type " + type);
		}

		double val1 = ((Number) obj1).doubleValue();
		double val2 = ((Number) obj2).doubleValue();

		if (logger.isTraceEnabled()) {
			logger.trace("Obj1 type " + obj1.getClass() + 
					"\tObj2 type " + obj2.getClass());
		}
		
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
			logger.trace("RETURN evaluate() with " + returnValue);
		}
		return returnValue;
	}
	
}
