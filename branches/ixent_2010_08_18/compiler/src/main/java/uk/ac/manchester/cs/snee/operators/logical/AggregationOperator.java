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
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;


/**
 * Aggregation operator.
 * Either for before the Aggreagtion is split in three 
 * Or as a superclass for the three seperate Operators.
 * 
 * @author Christian
 */
public class AggregationOperator extends PredicateOperator {

	private Logger logger = Logger.getLogger(AggregationOperator.class.getName());
	/** 
	 * List of the aggregation within the requested expression.
	 */
	private List<AggregationExpression> aggregates;
	
	/**
	 * @throws OptimizationException 
	 */
	public AggregationOperator(List <Expression> expressions, 
			List <Attribute> attributes, LogicalOperator inputOperator, 
			AttributeType boolType) 
	throws OptimizationException {
    	super(expressions, attributes, inputOperator, boolType);
        this.setOperatorName("AGGREGATION");
//        this.setNesCTemplateName("aggregation");
        setAggregates();
        if (this.getOperatorDataType() == OperatorDataType.STREAM) {
			String message = "Illegal attempt to place an " +
					"AggregationOperator on a Stream of Tuples.";
			logger.warn(message);
			throw new OptimizationException(message);
		} 
    }
    
//    /**
//     * Constuctor based on another operator.
//     * @param model Operator to copy values from.
//     */
//    public AggregationOperator(AggregationOperator model) {
//    	super(model);
//    	this.aggregates = model.aggregates;
//    }
// 
    /**
     * Extracts and saves the actual aggregates from within the expressions.
     * 
     * For example an expression could be max(temp) - avg(temp)
     * Which contains two aggregates. 
     */
    private void setAggregates() {
		boolean needsCount = false;
    	aggregates = new ArrayList<AggregationExpression>();
    	//Place an evalTime
    	aggregates.add(null);
    	for (int i = 0; i < getExpressions().size(); i++) {
    		List<AggregationExpression> newAggs 
    			=  getExpressions().get(i).getAggregates();
    		for (int j = 0; j < newAggs.size(); j++) {
    			if (newAggs.get(j).getAggregationType() 
    					== AggregationType.COUNT) {
    				needsCount = true;
    			} else {
        			if (newAggs.get(j).needsCount()) {
        				needsCount = true;
        			}
    				if (!aggregates.contains(newAggs.get(j))) {
    					aggregates.add(newAggs.get(j));
    				}
    			}
    		}
    	}
    	if (needsCount) {
    		//FIXME: The typing here is a bit iffy
    		AggregationExpression count 
    			= new AggregationExpression(null, AggregationType.COUNT, 
    					aggregates.get(0).getType()); 
    		aggregates.add(count);
    	}
    }
    
    /**
     * {@inheritDoc}
     * 
     * @return False Because any select to be done before aggregation 
     *    will already be below this operator.
     */
	 public boolean pushSelectDown(Expression predicate) {
		 return false;
	 }

    /**
     * gets the aggregates.
     * @return the Aggregates.
     */
    protected List<AggregationExpression> getAggregates() {
		return aggregates;
	}
    
    /**
     * {@inheritDoc}
     */
    public boolean pushProjectionDown(
    		List<Expression> projectExpressions, 
    		List<Attribute> projectAttributes) 
	throws OptimizationException {
    	
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

//	/** {@inheritDoc} */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		return 1;
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getCardinality(CardinalityType card, 
//			Site node, DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(1);
//    	return result;
//    }

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
 
//    /** {@inheritDoc} */
//     public AggregationOperator shallowClone() {
//    	AggregationOperator clonedOp = new AggregationOperator(this);
//    	return clonedOp;
//    }

    /**
     * Used to determine if the operator is Attribute sensitive.
     *
     * @return true.
     */
    public boolean isAttributeSensitive() {
        return true;
    }

//	private double getTimeCost(int tuples) {
//		return getOverheadTimeCost()
//				+ CostParameters.getCopyTuple() 
//				+ CostParameters.getDoCalculation() * tuples;
//	}

//    /** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card,
//			Site node, DAF daf) {
//		int tuples = this.getInputCardinality(card, node, daf, 0);
//		return getTimeCost(tuples);
//	}
	
//    /** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, int numberOfInstances) {
//		int tuples = this.getInputCardinality(card, 0, numberOfInstances);
//		return getTimeCost(tuples);
//	}
	
//    /** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(getOverheadTimeCost() + CostParameters.getCopyTuple());
//		AlphaBetaExpression tuples 
//			= this.getInputCardinality(card, node, daf, round, 0);
//		tuples.multiplyBy(CostParameters.getDoCalculation());
//		result.add(tuples);
//		return result;
//	}
	
	/** {@inheritDoc} */
	public boolean isRemoveable() {
		return false;
	}

    /** {@inheritDoc} */
    public boolean isRecursive() {
        return false;
    }
 


}
