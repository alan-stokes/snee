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
package uk.ac.manchester.cs.snee.operators.logical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;


/**
 * Aggregation operator.
 * 
 * @author Christian
 */
public class AggregationOperator extends PredicateOperator {

	private Logger logger = Logger.getLogger(AggregationOperator.class.getName());
	
	/** 
	 * List of aggregate expressions within the requested expression.
	 */
	private List<AggregationExpression> aggregates;
	
	/**
	 * 
	 * @param expressions The expressions for building the output attributes.
	 * @param attributes The names of the output attributes.
	 * @param inputOperator The incoming operator
	 * @param boolType
	 * @throws OptimizationException
	 */
	public AggregationOperator(List <Expression> expressions, 
			List <Attribute> attributes, LogicalOperator inputOperator, 
			AttributeType boolType) 
	throws OptimizationException {
    	super(expressions, attributes, inputOperator, boolType);
        this.setOperatorName("AGGREGATION");
        extractAggregates();
        
        if (this.getOperatorDataType() == OperatorDataType.STREAM) {
			String message = "Illegal attempt to place an " +
					"AggregationOperator on a Stream of Tuples.";
			logger.warn(message);
			throw new OptimizationException(message);
		} 
    }
    
    /**
     * Extracts primitive aggregates from within expressions.
     * 
     * For example an expression could be max(temp) - avg(temp)
     * Which contains two aggregates. 
     */
    private void extractAggregates() {
    	aggregates = new ArrayList<AggregationExpression>();

    	for (int i = 0; i < this.getExpressions().size(); i++) {
    		List<AggregationExpression> aggrList 
    			=  this.getExpressions().get(i).getAggregates();
    		for (int j = 0; j < aggrList.size(); j++) {
    			aggregates.add(aggrList.get(j));
    		}
    	}
    }

	/** {@inheritDoc} */    
	public String getParamStr() {
		StringBuffer outputBuffer = new StringBuffer();
		for (AggregationExpression aggExp : aggregates) {
			outputBuffer.append(aggExp.toString()).append(" ");
		}
		return outputBuffer.toString();
	}
    
    /**
     * {@inheritDoc}
     * 
     * @return False Because any select to be done before aggregation 
     *    will already be below this operator.
     */
	 public boolean pushSelectIntoLeafOp(Expression predicate) {
		 return false;
	 }

    /**
     * gets the aggregates.
     * @return the Aggregates.
     */
    public List<AggregationExpression> getAggregates() {
		return aggregates;
	}
    
    /**
     * {@inheritDoc}
     * @throws SNEEConfigurationException 
     */
    public boolean pushProjectionDown(
    		List<Expression> projectExpressions, 
    		List<Attribute> projectAttributes) 
	throws OptimizationException, SNEEConfigurationException {
    	
    	//TODO remove attributes not require upstream

    	List<Attribute> requiredAttributes = new ArrayList<Attribute>(); 
    	for (int i = 0; i < getExpressions().size(); i++) {
    		List<Attribute> requiredAttributesX = getExpressions().get(i).getRequiredAttributes();
    		requiredAttributes.addAll(requiredAttributesX);
    	}

    	getInput(0).pushProjectionDown(new  ArrayList<Expression>(), requiredAttributes);
    	
    	return false;
	}

	/** {@inheritDoc} */
	public int getCardinality(CardinalityType card) {
		return 1;
	}


//	/**
//	 * Converts an Expression to an output Attribute.
//	 * @param e Expression which must be an AggregationExpression
//	 * @return An Attribute representing the result of the expression
//	 * @throws SchemaMetadataException 
//	 */
//    private DataAttribute convertToAttribute(Expression e) 
//    throws SchemaMetadataException {
//    	AggregationExpression agg = (AggregationExpression) e; 
//        return new DataAttribute(agg.getShortName(), e.getType());
//    }

    /**
     * Used to determine if the operator is Attribute sensitive.
     *
     * @return true.
     */
    public boolean isAttributeSensitive() {
        return true;
    }

	/** {@inheritDoc} */
	public boolean isRemoveable() {
		return false;
	}

    /** Looks at all the aggregation functions used in the operator, 
     * and considers whether they could be computed incrementally. **/
	public boolean isSplittable() {
		Iterator<AggregationExpression> aggrExprIter =
			aggregates.iterator();
		while (aggrExprIter.hasNext()) {
			AggregationExpression aggrExpr = aggrExprIter.next();
			if (aggrExpr.canBeDoneIncrementally()==false)
				return false;
		}
		return true;
	}

}
