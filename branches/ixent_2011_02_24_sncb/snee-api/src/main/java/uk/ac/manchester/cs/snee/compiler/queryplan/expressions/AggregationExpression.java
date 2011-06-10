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

import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;

/** Expression to hold an aggregation. */
public class AggregationExpression implements Expression {

	/** 
	 * Keeps track of the next aggregation numeration to assign.
	 */
	private static int aggrCount = 1;
	
	/**
	 * The aggrID assigned to this aggregation.
	 * Used to assign short unique local names. 
	 */
	private int aggrID;
	
	/** The expression over which the aggregation will be done. */
	private Expression inputExpression;
	
	/** The aggregation to be done. */
	private AggregationFunction aggrFunction;

	private AttributeType _returnType;
	
	public AggregationExpression(Expression inner, 
			AggregationFunction aggrFn, AttributeType returnType) {
        inputExpression = inner;
        this.aggrFunction = aggrFn;
        _returnType = returnType;
        aggrID = aggrCount++;
	}

    /** {@inheritDoc}*/
	public List<Attribute> getRequiredAttributes() {
		return inputExpression.getRequiredAttributes();
	}
	
	/** {@inheritDoc} */
	public AttributeType getType() {
		return _returnType;
	}
	
	/** 
	 * Gets the type of the aggregation.
	 * @return The type of the aggregation.
	 */
	public AggregationFunction getAggregationFunction() {
		return aggrFunction;
	}
	
	/** 
	 * Checks if any of the aggregates need count.
	 * For example incremental Average and Count both need a count kept.
	 * 
	 * @return TRUE if one or more of the aggregates need a count kept.
	 */
	public boolean needsCount() {
		if (aggrFunction == AggregationFunction.AVG) {
			return true;
		} 
		if (aggrFunction == AggregationFunction.COUNT) {
			return true;
		}
		return false;
	}

	/** 
	 * Checks if any of the aggregates need sum.
	 * For example incremental Average and SUM both need a sum kept.
	 * 
	 * @return TRUE if one or more of the aggregates need a count kept.
	 */
	public boolean needsSum() {
		if (aggrFunction == AggregationFunction.AVG) {
			return true;
		} 
		if (aggrFunction == AggregationFunction.SUM) {
			return true;
		}
		return false;
	}
	
	/* Used to decide whether an aggregation operator can be split */
	public boolean canBeDoneIncrementally() {
		if ((aggrFunction == AggregationFunction.AVG) || (aggrFunction == AggregationFunction.COUNT) ||
				(aggrFunction == AggregationFunction.SUM) || (aggrFunction == AggregationFunction.MIN) ||
				(aggrFunction == AggregationFunction.MAX)){
			return true;
		}
		return false;
	}
	
	
	/** {@inheritDoc}*/
	public String toString() {
		return (aggrFunction + "(" + inputExpression + ")");
	}
	
	/**
	 * Assign a value to any intermediate values 
	 * used to represent partial results. 
	 * @return A unique string name for this attribute.
	 */
	public String getShortName() {
		if (aggrFunction == AggregationFunction.COUNT) {
			return "count";
		}
		return aggrFunction.toString() + aggrID;		
	}

	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return An array List of all the aggregates within this expressions.
	 * Could contain duplicates.
	 */
	public List<AggregationExpression> getAggregates()	{
		List<AggregationExpression> list 
			= new ArrayList<AggregationExpression>(1);
		list.add(this);
		return list;
	}

	/** 
	 * Gets the expression inside the aggregate.
	 * @return The input expression.
	 */
	public Expression getExpression() {
		return inputExpression;
	}

	/**
	 * Finds the minimum value that this expression can return.
	 * @return The minimum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMinValue() {
    	throw new AssertionError("Illegal call to getMinValue");
		//return 0;
	}
	
	/**
	 * Finds the maximum value that this expression can return.
	 * @return The maximum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMaxValue() {
    	throw new AssertionError("Illegal call to getMaxValue");
	}
	
	/**
	 * Finds the expected selectivity of this expression can return.
	 * @return The expected selectivity
	 * @throws AssertionError If Expression does not return a boolean.
	 */
	public double getSelectivity() {
    	throw new AssertionError("Illegal call to getSelectivity");
	}

	/**
	 * Checks if the Expression can be directly used in an Aggregation Operator.
	 * Expressions such as attributes that can only be used inside a aggregation expression return false.
	 * 
	 * @return true if this expression can be directly used in a Aggregation Operator. 
	 * @throws ParserValidationException 
	 */
	public boolean allowedInAggregationOperator() throws ExpressionException {
		if (inputExpression.allowedInProjectOperator())
			return true;
		throw new ExpressionException("Aggregate: " + this.aggrFunction +
				" not allowed over Expression " + inputExpression);
	}

	/**
	 * Checks if the Expression can be used in a Project Operator.
	 * 
	 * @return false 
	 */
	public boolean allowedInProjectOperator(){
		return false;
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
		Attribute innerAttr = inputExpression.getRequiredAttributes().get(0);
		DataAttribute attribute = new DataAttribute(innerAttr.getExtentName(), innerAttr.getAttributeSchemaName()+"_"
			+aggrFunction.toString(), this.getType());
		attribute.setIsConstant(false);
		return attribute;
	}
	
	public void setIsConstant(boolean b) {
		throw new AssertionError("An aggregation expression cannot be a constant.");
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression#isConstant()
	 * An aggregation expression cannot be a constant
	 */
	public boolean isConstant() {
		return false;
	}
}
