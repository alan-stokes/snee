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

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;

/**
 * Select operator for the situations the predicates can not be pushed down
 *  into the next operator.
 * @author Christian
 *
 */
public class SelectOperator extends LogicalOperatorImpl {    

	private Logger logger =
		Logger.getLogger(SelectOperator.class.getName());
	
	/**
	 * Constructs a new Select Operator.
	 * Called because previous operator does not accept predicates.
	 * @param predicate Predicate to apply
	 * @param inputOperator Previous Operator.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public SelectOperator(Expression predicate, LogicalOperator inputOperator,
			AttributeType boolType) 
	throws SchemaMetadataException, AssertionError, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SelectOperator() with " + predicate +
					" " + inputOperator);

		this.setOperatorName("SELECT");
//		this.setNesCTemplateName("select");
		setOperatorDataType(inputOperator.getOperatorDataType());

		setChildren(new LogicalOperator[] {inputOperator});

		setPredicate(predicate);
		this.setParamStr(getPredicate().toString());
		if (logger.isDebugEnabled())
			logger.debug("RETURN SelectOperator() " + this);
	}  

	//used by clone method
//	/**
//	 * Constructor that creates a new operator 
//	 * based on a model of an existing operator.
//	 * 
//	 * Used by both the clone method and the constructor of the physical methods.
//	 * @param model Operator to copy values from.
//	 */
//	protected SelectOperator(SelectOperator model) {
//		super(model);
//	}  

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		return false;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * Push new and old predicate down as required.
	 * 
	 * @return True if able to push predicate down.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException {
		if (predicate instanceof MultiExpression)
			return combineSelects(predicate);
		assert(predicate instanceof NoPredicate);
		Expression oldPredicate = this.getPredicate();
		if (this.getInput(0).pushSelectDown(oldPredicate)) {
			setPredicate (new NoPredicate());
			return true;
		}
		return false;
	}

	/**
	 * Attempts to combine new predicate with an existing one.
	 *  
	 * @return The result of the push to the child.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private boolean combineSelects(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER combineSelects() with " + predicate);
		boolean result = false;
		assert(predicate instanceof MultiExpression);
		MultiExpression thePredicate = (MultiExpression)predicate;
		assert(thePredicate.getMultiType().isBooleanDataType());
		Expression oldPredicate = this.getPredicate();
		if (oldPredicate instanceof NoPredicate) {
			if (logger.isTraceEnabled())
				logger.trace("Instance of NoPredicate");
			if (this.getInput(0).pushSelectDown(predicate))
				result = true;
			else
				result = checkAndSetPredicate(predicate);
		} else if (oldPredicate instanceof MultiExpression) {
			if (logger.isTraceEnabled())
				logger.trace("Instance of MultiExpression");
			MultiExpression oldPredicate2 = 
				(MultiExpression) oldPredicate;
			Expression combined = 
				oldPredicate2.combinePredicates(oldPredicate2, 
						thePredicate);
			if (this.getInput(0).pushSelectDown(combined)) {
				setPredicate (new NoPredicate());
				result = true;
			} else {
				if (this.getInput(0).pushSelectDown(predicate)) 
					result = true;
				else if (this.getInput(0).pushSelectDown(oldPredicate)) {
					result = checkAndSetPredicate(predicate);
				} else
					result = false;		 
			}
		} else {
			String message = "Unexpected Predicate type";
			logger.warn(message);
			throw new AssertionError(message);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN combineSelects() with " + result);
		return result;
	 }

	 private boolean checkAndSetPredicate (Expression predicate) 
	 throws SchemaMetadataException, AssertionError, 
	 TypeMappingException 
	 {
		 if (logger.isTraceEnabled())
			 logger.trace("ENTER checkAndSetPredicate() with " + predicate);
		 List<Attribute> required = predicate.getRequiredAttributes();
		 List<Attribute> available = this.getAttributes();
		 for (int i = 0; i < required.size(); i++)
			 if (!available.contains(required.get(i))) {
				 if (logger.isTraceEnabled())
				 	logger.trace("RETURN checkAndSetPredicate() with " + false);
				 return false;
			 }
		 setPredicate(predicate);
		 if (logger.isTraceEnabled())
			 logger.trace("RETURN checkAndSetPredicate() with " + true);
		 return true;
	 }
	 
	 /** 
	  * {@inheritDoc}
	  * Should never be called as there is always a project or aggregation 
	  * between this operator and the rename operator.
	  */   
	 public void pushLocalNameDown(String newLocalName) {
		 String message = "Unexpected call to pushLocalNameDown()";
		 logger.warn(message);
		 throw new AssertionError(message); 
	 }

	/**
	 * Calculated the cardinality based on the requested type.
	 * Currently very crude. 
	 * 
	 * @param card Type of cardinality to be considered.
	 * 
	 * @return The Cardinality calculated as requested.
	 */
	public int getCardinality(CardinalityType card) {
		int input = getInput(0).getCardinality(card);
        if ((card == CardinalityType.MAX) 
        		|| (card == CardinalityType.PHYSICAL_MAX)) {
			return input;
        } 
        if ((card == CardinalityType.AVERAGE) 
        		|| (card == CardinalityType.MAX)) {
			return input / Constants.JOIN_PREDICATE_SELECTIVITY;
		}
        if (card == CardinalityType.MINIMUM) {
    		return 0;
        } 
        throw new AssertionError("Unexpected CardinaliyType " + card);
	}

//	/** {@inheritDoc} */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		int input = getInputCardinality(card, node, daf, 0);
//        if ((card == CardinalityType.MAX) 
//        		|| (card == CardinalityType.PHYSICAL_MAX)) {
//			return input;
//        } 
//        if ((card == CardinalityType.AVERAGE) 
//        		|| (card == CardinalityType.MAX)) {
//			return input / Constants.JOIN_PREDICATE_SELECTIVITY;
//		}
//        if (card == CardinalityType.MINIMUM) {
//    		return 0;
//        } 
//        throw new AssertionError("Unexpected CardinaliyType " + card);
//	}
	
//	/** {@inheritDoc} */
//	public AlphaBetaExpression getCardinality(CardinalityType card, 
//			Site node, DAF daf, boolean round) {
//		AlphaBetaExpression input = 
//			getInputCardinality(card, node, daf, round, 0);
//        if ((card == CardinalityType.MAX) 
//        		|| (card == CardinalityType.PHYSICAL_MAX)) {
//			return input;
//        } 
//        if ((card == CardinalityType.AVERAGE) 
//        		|| (card == CardinalityType.MAX)) {
//        	input.divideBy(Constants.JOIN_PREDICATE_SELECTIVITY);
//			return input;
//		}
//        if (card == CardinalityType.MINIMUM) {
//    		return new AlphaBetaExpression();
//        } 
//        throw new AssertionError("Unexpected CardinaliyType " + card);
//	}

	/**
	 * Used to determine if the operator is Attribute sensitive.
	 * 
	 * @return false.
	 */
	public boolean isAttributeSensitive() {
		return false;
	}

	/** {@inheritDoc} */
	public boolean isLocationSensitive() {
		return false;
	}

    /** {@inheritDoc} */
	public boolean isRecursive() {
		return false;
	}
	
    /** {@inheritDoc} */
    public boolean acceptsPredicates() {
        return true;
    }
    
    /** {@inheritDoc} */
	public String toString() {
		return this.getText() + "[ " + super.getInput(0).toString() + " ]";
    }

//    /** {@inheritDoc} */
//	public SelectOperator shallowClone() {
//		SelectOperator clonedOp = new SelectOperator(this);
//		return clonedOp;
//	}
	
//    private double getTimeCost(int tuples) {
//		return getOverheadTimeCost()
//			+ (CostParameters.getCopyTuple() 
//			+ CostParameters.getApplyPredicate()) * tuples;
//    }
    
//    /** {@inheritDoc} */
//    public double getTimeCost(CardinalityType card, 
//    		Site node, DAF daf) {
//		int tuples = this.getInputCardinality(card, node, daf, 0);
//		return getTimeCost(tuples);
//    }

//    /** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, int numberOfInstances) {
//		int tuples = this.getInputCardinality(card, 0, numberOfInstances);
//		return getTimeCost(tuples);
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(getOverheadTimeCost());
//		AlphaBetaExpression tuples 
//			= this.getInputCardinality(card, node, daf, round, 0);
//		tuples.multiplyBy(CostParameters.getCopyTuple() 
//				+ CostParameters.getApplyPredicate());
//		result.add(tuples);
//		return result;
//	}
	
     /** {@inheritDoc} */
    public boolean isRemoveable() {
    	if (getPredicate() instanceof NoPredicate)
    		return true;
    	return false;
    }

    //Call to default methods in OperatorImplementation

//    /** {@inheritDoc} */
//    public int[] getSourceSites() {
//    	return super.defaultGetSourceSites();
//    }

//	/** {@inheritDoc} */    
//    public int getOutputQueueCardinality(Site node, DAF daf) {
//    	return super.defaultGetOutputQueueCardinality(node, daf);
//    }

// 	/** {@inheritDoc} */    
//    public int getOutputQueueCardinality(int numberOfInstances) {
//    	return super.defaultGetOutputQueueCardinality(numberOfInstances);
//    }

	/** {@inheritDoc} */    
    public List<Attribute> getAttributes() {
    	return super.defaultGetAttributes();
    }

    /** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

//	/** {@inheritDoc} */    
//	public int getDataMemoryCost(Site node, DAF daf) {
//		return super.defaultGetDataMemoryCost(node, daf);
//	}

//    /**
//     * Displays the results of the cost functions.
//     * @param node Physical mote on which this operator has been placed.
//     * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//     */
//	public double getTimeCost2(Site node, DAF daf) {
//		return SharedCostFunctions.dSelect(getPredicate(), getExpressions(), 
//				getCardinality(CardinalityType.MAX, node, daf)); 
//	}

//    /**
//     * Displays the results of the cost functions.
//     * @param node Physical mote on which this operator has been placed.
//     * @param daf Distributed query plan this operator is part of.
//     * @return OutputQueueCardinality * PhytsicalTuplesSize
//     */
//	public double getEnergyCost2(Site node, DAF daf) {
//		return SharedCostFunctions.eSelect(getPredicate(), getExpressions(), 
//				getCardinality(CardinalityType.MAX, node, daf)); 
//	}

//	/**
//     * Displays the results of the cost functions.
//     * @param node Physical mote on which this operator has been placed.
//     * @param daf Distributed query plan this operator is part of.
//     * @return OutputQueueCardinality * PhytsicalTuplesSize
//     */
//	public int getDataMemoryCost2(Site node, DAF daf) {
//		return SharedCostFunctions.mSelect(
//				getExpressions(), 
//				getCardinality(CardinalityType.MAX, node, daf)); 
//	}


}
