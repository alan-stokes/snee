/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.compiler.queryplan.expressions;

import java.util.ArrayList;
import java.util.List;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/**
 * Expression combining two or more boolean expression.
 * @author Christian
 */
public class MultiExpression implements Expression {

	/** List of the expression.*/
	private Expression[] expressions;

	/** The method of combining the expressions.*/
	private MultiType multiType;

	private AttributeType _booleanType;
	
	/**
	 * A multi-expression can only be a constant if all its parts are constants
	 */
	private boolean isConstant = false;
	
	private boolean isJoinCondition = false;

	/**
	 * Constuctor.
	 * 
	 * @param newExpressions Expressions inside the AND
	 * @param type The Type of combination or expressions to do.
	 * @param booleanType The type being used for BOOLEANS as read in from the types file
	 */
	public MultiExpression(Expression[] newExpressions, 
			MultiType type, AttributeType booleanType) {
		this.expressions = newExpressions;
		assert (type == MultiType.SQUAREROOT || expressions.length >= 2);
		assert (type != null);
		this.multiType = type;
		_booleanType = booleanType;
		calculateIsConstant();
		calculateIsJoin();
	}
	
	private void calculateIsJoin() {
		switch (multiType) {
		case AND:
		case OR:
			// Check sub-expressions
				isJoinCondition = expressions[0].isJoinCondition();
			break;
		case EQUALS:
		case GREATERTHAN:
		case GREATERTHANEQUALS:
		case LESSTHAN:
		case LESSTHANEQUALS:
		case NOTEQUALS:
			// Check sources of attributes			
			Expression exp1 = expressions[0];
			Expression exp2 = expressions[1];
			if (exp1.isConstant() || exp2.isConstant()) {
				// One of the expressions is a constant value
				isJoinCondition = false;
			} else {
				String extent1 = 
					exp1.getRequiredAttributes().get(0).getExtentName();
				String extent2 = 
					exp2.getRequiredAttributes().get(0).getExtentName();
				if (extent1.equalsIgnoreCase(extent2)) {
					// Comparison of attributes from same extent
					isJoinCondition = false;
				} else {
					isJoinCondition = true;
				}
			}
			break;
		default:
			// All other cases are false be default, no action needed
			break;
		}
	}

	private void calculateIsConstant() {
		boolean result = true;
		for (Expression exp : expressions) {
			if (!exp.isConstant()) {
				result = false;
				break;
			}
		}
		setIsConstant(result);
	}

	public MultiExpression combinePredicates (MultiExpression first, 
			MultiExpression second) {
		if (second.getMultiType() != MultiType.AND)
			return combineAndToOther(first, second);
		if (first.getMultiType() != MultiType.AND)
			return combineAndToOther(second, first);
		Expression[] combine = new Expression[first.expressions.length + 
		                                      second.expressions.length];
		for (int i = 0; i < first.expressions.length; i++) {
			combine[i] = first.expressions[i];
		}
		for (int i = 0; i < second.expressions.length; i++)
			combine[i+first.expressions.length] = second.expressions[i];
		return new MultiExpression (combine, MultiType.AND, _booleanType);
	}

	private MultiExpression combineAndToOther (MultiExpression first, 
			MultiExpression second) {
		if (first.getMultiType() != MultiType.AND)
			return combineOtherToOther(first, second);
		assert(second.getMultiType().isBooleanDataType());
		Expression[] combine = new Expression[first.expressions.length + 1];
		for (int i = 0; i < first.expressions.length; i++)
			combine[i] = first.expressions[i];
		combine[first.expressions.length] = second;
		return new MultiExpression (combine, MultiType.AND, _booleanType);
	}

	private MultiExpression combineOtherToOther (MultiExpression first, 
			MultiExpression second) {
		assert(first.getMultiType().isBooleanDataType());
		assert(second.getMultiType().isBooleanDataType());
		Expression[] combine = new Expression[2];
		combine[0] = first;
		combine[1] = second;
		return new MultiExpression (combine, MultiType.AND, _booleanType);
	}

//	/**
//	 * Converts an AST token into a multiType.
//	 * @param token The AST token of how the expressions are combined.
//	 */
//	private void convertMultiType(AST token) {
//		//FIXME: Method never used locally
//        switch (token.getType()) {
//        	case SNEEqlOperatorParserTokenTypes.ADD: 
//        		multiType = MultiType.ADD;
//        		assert (expressions.length >= 2);
//        		return;
//        	case SNEEqlOperatorParserTokenTypes.AND: 
//    			multiType = MultiType.AND;
//        		assert (expressions.length >= 2);
//    			return;
//        	case SNEEqlOperatorParserTokenTypes.OR: 
//    			multiType = MultiType.OR;
//        		assert (expressions.length >= 2);
//    			return;
//        	case SNEEqlOperatorParserTokenTypes.DIVIDE: 
//				multiType = MultiType.DIVIDE;
//        		assert (expressions.length == 2);
//				return;
//        	case SNEEqlOperatorParserTokenTypes.EQUALS:
//    			multiType = MultiType.EQUALS;
//        		assert (expressions.length == 2);
//    			return;
//        	case SNEEqlOperatorParserTokenTypes.GREATERTHAN:
//        		assert (expressions.length == 2);
//				multiType = MultiType.GREATERTHAN;
//				return;
//        	case SNEEqlOperatorParserTokenTypes.GREATERTHANOREQUALS: 
//				multiType = MultiType.GREATERTHANEQUALS;
//        		assert (expressions.length == 2);
//				return;
//        	case SNEEqlOperatorParserTokenTypes.LESSTHAN:
//				multiType = MultiType.LESSTHAN;
//        		assert (expressions.length == 2);
//				return;
//        	case SNEEqlOperatorParserTokenTypes.LESSTHANOREQUALS:
//				multiType = MultiType.LESSTHANEQUALS;
//        		assert (expressions.length == 2);
//				return;
//        	case SNEEqlOperatorParserTokenTypes.MULTIPLY:
//				multiType = MultiType.MULTIPLY;
//        		assert (expressions.length >= 2);
//        		return;
//        	case SNEEqlOperatorParserTokenTypes.MINUS:
//				multiType = MultiType.MINUS;
//        		assert (expressions.length >= 2);
//        		return;
//        	case SNEEqlOperatorParserTokenTypes.POWER:
//				multiType = MultiType.POWER;
//        		assert (expressions.length == 2);
//        		return;
//        	case SNEEqlOperatorParserTokenTypes.SQUAREROOT:
//				multiType = MultiType.SQUAREROOT;
//        		assert (expressions.length == 1);
//        		return;
//        	default: throw new AssertionError("Unexpected AST token " + token); 
//        }
//	}
	
	/** {@inheritDoc} */
	public String toString() {
		String output = expressions[0].toString();
		for (int i = 1; i < expressions.length; i++) {
			output = output + multiType.toString() + expressions[i].toString();
		}
		return output;
	}

	/** {@inheritDoc} */
	public List<Attribute> getRequiredAttributes() {
		List<Attribute> required = expressions[0].getRequiredAttributes();
		for (int i = 1; i < expressions.length; i++) {
			List<Attribute> more = expressions[i].getRequiredAttributes();
			for (int j = 0; j < more.size(); j++) {
				if (!required.contains(more.get(j))) {
					required.add(more.get(j));
				}
			}
		}
		return required;
	}

	/** Gets the data type returned by this expression. 
	 * @return The data type of this expression.
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public AttributeType getType() 
	throws SchemaMetadataException, TypeMappingException 
	{
		AttributeType returnType;
		if (multiType.isBooleanDataType()) {
			returnType = _booleanType;
		} else {
			//simplified version assumes all types are the same.
			returnType = expressions[0].getType();
		}
		return returnType;
	}

	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return An array List of all the aggregates within this expressions.
	 * Could contain duplicates.
	 */
	public List<AggregationExpression> getAggregates()	{
		ArrayList<AggregationExpression> list 
			= new ArrayList<AggregationExpression>(1);
		for (int i = 0;  i < expressions.length; i++) {
			list.addAll(expressions[i].getAggregates());			
		}
		return list;
	}
	
	/** 
	 * Accessor.
	 * @return List of the expressions being combined.
	 */
	public Expression[] getExpressions() {
		return expressions;
	}
	
	/**
	 * Accessor.
	 * @return The type of combination being done.
	 */
	public MultiType getMultiType() {
		assert (multiType != null);
		return multiType;
	}
	
	/**
	 * Finds the minimum value that this expression can return.
	 * @return The minimum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMinValue() {
		double temp;
		double temp1;
		switch (multiType) {
    	case ADD:
    		temp = 0;
    		for (int i = 0; i < expressions.length; i++){
    			temp = temp + expressions[1].getMinValue(); 
    		}
    		return temp; 
    	case AND:
    		throw new AssertionError("Illegal call to getMinValue with Operator AND");
    	case DIVIDE:	
    		temp = expressions[0].getMinValue();
    		temp1 = expressions[1].getMinValue();
    		assert(temp >= 0);
    		assert(temp1 >= 0);
    		if (temp1 == 0) {
        		throw new AssertionError("Possible divide by zero with Operator DIVIDE");
    		}
   			return temp/ temp1; 
    	case EQUALS:
    		throw new AssertionError("Illegal call to getMinValue with Operator EQUALS");
    	case GREATERTHAN:
    		throw new AssertionError("Illegal call to getMinValue with Operator GREATERTHAN");
    	case GREATERTHANEQUALS:
    		throw new AssertionError("Illegal call to getMinValue with Operator GREATERTHANEQUALS");
    	case LESSTHAN:
    		throw new AssertionError("Illegal call to getMinValue with Operator LESSTHAN");
		case LESSTHANEQUALS:
			throw new AssertionError("Illegal call to getMinValue with Operator LESSTHANEQUALS");
		case MULTIPLY:
			assert(expressions[0].getMinValue() >= 0);
			assert(expressions[1].getMinValue() >= 0);
			return expressions[0].getMinValue() * expressions[1].getMinValue();
		case NOTEQUALS:
			throw new AssertionError("Illegal call to getMinValue with Operator NOTEQUALS");
    	default: throw new AssertionError("Unexpected operator in getMinValue");
		}
	}
	
	/**
	 * Finds the maximum value that this expression can return.
	 * @return The maximum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMaxValue() {
		double temp;
		double temp1;
		switch (multiType) {
    	case ADD:
    		temp = 0;
    		for (int i = 0; i < expressions.length; i++){
    			temp = temp + expressions[1].getMaxValue(); 
    		}
    		return temp; 
    	case AND:
    		throw new AssertionError("Illegal call to getMaxValue with Operator AND");
    	case DIVIDE:	
    		temp = expressions[0].getMaxValue();
    		temp1 = expressions[1].getMaxValue();
    		assert(temp >= 0);
    		assert(temp1 >= 0);
    		if (temp1 == 0) {
        		throw new AssertionError("Possible divide by zero with Operator DIVIDE");
    		}
   			return temp * temp1; 
    	case EQUALS:
    		throw new AssertionError("Illegal call to getMaxValue with Operator EQUALS");
    	case GREATERTHAN:
    		throw new AssertionError("Illegal call to getMaxValue with Operator GREATERTHAN");
    	case GREATERTHANEQUALS:
    		throw new AssertionError("Illegal call to getMaxValue with Operator GREATERTHANEQUALS");
    	case LESSTHAN:
    		throw new AssertionError("Illegal call to getMaxValue with Operator LESSTHAN");
		case LESSTHANEQUALS:
			throw new AssertionError("Illegal call to getMaxValue with Operator LESSTHANEQUALS");
		case MULTIPLY:
			assert(expressions[0].getMaxValue() >= 0);
			assert(expressions[1].getMaxValue() >= 0);
			return expressions[0].getMaxValue() * expressions[1].getMaxValue();
		case NOTEQUALS:
			throw new AssertionError("Illegal call to getMaxValue with Operator NOTEQUALS");
    	default: throw new AssertionError("Unexpected operator in getMaxValue");
		}
	}
	
	/**
	 * Finds the expected selectivity of this expression can return.
	 * @return The expected selectivity
	 * @throws AssertionError If Expression does not return a boolean.
	 */
	public double getSelectivity() {
		double temp;
		double range;
		double range1;
		switch (multiType) {
    	case ADD:
    		throw new AssertionError("Illegal call to getSelectivity() with Operator ADD");
    	case AND:
    		temp = 1;
    		for (int i = 0; i < expressions.length; i++){
    			temp = temp * expressions[1].getSelectivity(); 
    		}
    		return temp; 
    	case DIVIDE:	
    		throw new AssertionError("Illegal call to getSelectivity() with Operator DIVIDE");
    	case EQUALS:
    		if (expressions[0].getMaxValue() < expressions[1].getMinValue()) {
    			return 0;
    		}
    		if (expressions[1].getMaxValue() < expressions[0].getMinValue()) {
    			return 0;
    		}
    		range = expressions[0].getMaxValue() - expressions[0].getMinValue();
    		range1 = expressions[1].getMaxValue() - expressions[1].getMinValue();
    		if (range == 0) {
    			if (range1 == 0) {
    				if (expressions[0].getMaxValue() == expressions[1].getMinValue()) {
    					return 1;
    				}
    				return 0;
    			}
    			return 1/range1; 
    		}
			if (range1 == 0) {
				return 1/range;
			}	
    		return (1/(range * range1));
    	case GREATERTHAN:
    		if (expressions[0].getMaxValue() <= expressions[1].getMinValue()) {
    			return 0;
    		}
    		if (expressions[1].getMaxValue() < expressions[0].getMinValue()) {
    			return 1;
    		}
    		//Not yet finished use defualt
    		return 1/Constants.JOIN_PREDICATE_SELECTIVITY;
    	case GREATERTHANEQUALS:
    		if (expressions[0].getMaxValue() < expressions[1].getMinValue()) {
    			return 0;
    		}
    		if (expressions[1].getMaxValue() <= expressions[0].getMinValue()) {
    			return 1;
    		}
    		//Not yet finished use defualt
    		return 1/Constants.JOIN_PREDICATE_SELECTIVITY;
    	case LESSTHAN:
    		if (expressions[0].getMaxValue() < expressions[1].getMinValue()) {
    			return 1;
    		}
    		if (expressions[1].getMaxValue() <= expressions[0].getMinValue()) {
    			return 0;
    		}
    		//Not yet finished use defualt
    		return 1/Constants.JOIN_PREDICATE_SELECTIVITY;
		case LESSTHANEQUALS:
    		if (expressions[0].getMaxValue() <= expressions[1].getMinValue()) {
    			return 1;
    		}
    		if (expressions[1].getMaxValue() < expressions[0].getMinValue()) {
    			return 0;
    		}
    		//Not yet finished use defualt
    		return 1/Constants.JOIN_PREDICATE_SELECTIVITY;
		case MULTIPLY:
    		throw new AssertionError("Illegal call to getSelectivity() with Operator MULTIPLY");
		case NOTEQUALS:
    		//Not yet finished use defualt
    		return 1/Constants.JOIN_PREDICATE_SELECTIVITY;
    	default: throw new AssertionError("Unexpected operator in getMaxValue");
		}
	}

	/**
	 * Checks if the Expression can be directly used in an Aggregation Operator.
	 * Expressions such as attributes that can only be used inside a aggregation expression return false.
	 * 
	 * @return true If and only if each child expression can be used.  
	 * @throws ParserValidationException 
	 */
	public boolean allowedInAggregationOperator() throws ExpressionException{
		for (int i = 0; i < expressions.length; i++){
			if (!expressions[i].allowedInAggregationOperator())
				return false;
		}	
		return true;	
	}

	/**
	 * Checks if the Expression can be used in a Project Operator.
	 * 
	 * @return true If and only if each child expression can be used.  
	 */
	public boolean allowedInProjectOperator(){
		for (int i = 0; i < expressions.length; i++){
			if (!expressions[i].allowedInProjectOperator())
				return false;
		}	
		return true;	
	}
	
	/**
	 * Converts this Expression to an Attribute.
	 * 
	 * @return The Attribute returned by this Expression.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public Attribute toAttribute() 
	throws SchemaMetadataException, TypeMappingException{
		DataAttribute attribute = 
			new DataAttribute("", this.toString(), getType());
		attribute.setIsConstant(isConstant);
		return attribute; 
	}

	@Override
	public boolean isConstant() {
		return isConstant;
	}

	@Override
	public void setIsConstant(boolean isConstant) {
		this.isConstant = isConstant;
	}

	@Override
	public boolean isJoinCondition() {
		return isJoinCondition;
	}

	@Override
	public void setIsJoinCondition(boolean isJoinCondition) {
		this.isJoinCondition = isJoinCondition;
	}

}
