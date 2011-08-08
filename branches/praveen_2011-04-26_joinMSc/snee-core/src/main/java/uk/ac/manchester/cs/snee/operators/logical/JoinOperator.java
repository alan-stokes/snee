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

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/** 
 * A Join or Cross product operator.
 * 
 * Will join exactly two inputs
 *
 */
public class JoinOperator extends LogicalOperatorImpl implements LogicalOperator {

   private Logger logger = Logger.getLogger(JoinOperator.class.getName());
   
   /**
    * operator to work in Nested Loop Join Mode
    */
   public static String NLJ_MODE = "NLJ_MODE";
   /**
    * operator to work in Symmetric Hash Join Mode
    */
   public static String SHJ_MODE = "SHJ_MODE";
   /**
    * operator to work in Simple Hash Join Mode
    */
   public static String HJ_MODE = "HJ_MODE";
   /**
    * operator to work in Simple Hash Join Mode
    * with the left operand being hashed
    */
   public static String LHJ_MODE = "LHJ_MODE";
   /**
    * operator to work in Simple Hash Join Mode
    * with the right operand being hashed
    */
   public static String RHJ_MODE = "RHJ_MODE";

   /** 
	 * List of the expressions to create the output attributes.
	 * 
	 * attributes used if not set/
	 */
	private List <Expression> expressions = null;;

	/**
	 * List of the attributes output by this operator.
	 */
	private List <Attribute> attributes;

	private boolean rightChildIsRelation = false;
	
	private boolean leftChildIsRelation = false;
	
	private String joinMode;
	
	
	
//	/**
//	 * Constructor that creates a new operator
//	 * based on a model of an existing operator.
//	 *
//	 * Used by both the clone method and the constuctor of the physical methods.
//	 *
//	 * @param model Another JoinOperator 
//	 *   upon which this new one will be cloned.
//	 */
//	protected JoinOperator(JoinOperator model) {
//		super(model);
//		this.expressions = model.expressions;
//		this.attributes = model.attributes;
//	}

	/**
	 * Non Haskell constructor.
	 * @param rootOperator Left input
	 * @param right Second input operator
	 * @throws OptimizationException 
	 * @throws SchemaMetadataException 
	 */
	public JoinOperator(LogicalOperator left, LogicalOperator right, AttributeType boolType) 
	throws OptimizationException, SchemaMetadataException {
		super(boolType);
		this.setOperatorName("JOIN");
//		this.setNesCTemplateName("join");

		/* This is overridden when the 
		 * child operators are a STREAM and RELATION */
		setOperatorDataType(OperatorDataType.WINDOWS);

		setChildren(left, right);
		setOperatorSourceType(getOperatorSourceType(left, right));
		this.setSourceRate(getSourceRate(left, right));
		setAlgorithm(NLJ_MODE);
		this.setGetDataByPullModeOperator(false);
		this.setPushBasedOperator(false);
		getIncomingAttributes();
	}

	

	private void setChildren (LogicalOperator left, LogicalOperator right) 
	throws OptimizationException {
		String message = "Illegal attempt to Join Stream of tuples with " +
			"anything but a Relation.";
		if (left.getOperatorDataType() == OperatorDataType.STREAM) {
			if (right.getOperatorDataType() == OperatorDataType.RELATION) {
				setChildren(new LogicalOperator[] {left, right});
				setOperatorDataType(OperatorDataType.STREAM);
				rightChildIsRelation = true;
			} else { 
				logger.warn(message);
				throw new OptimizationException(message); 
			}
		} else if (left.getOperatorDataType() == OperatorDataType.WINDOWS) {
			if ((right.getOperatorDataType() == OperatorDataType.WINDOWS) ||  
					(right.getOperatorDataType() == OperatorDataType.RELATION)) {
				setChildren(new LogicalOperator[] {left, right});
				setOperatorDataType(OperatorDataType.WINDOWS);
				if (right.getOperatorDataType() == OperatorDataType.RELATION) {
					rightChildIsRelation = true;
				}
			} else {
				logger.warn(message);
				throw new OptimizationException(message);
			}
		} else if (left.getOperatorDataType() == OperatorDataType.RELATION) {
			rightChildIsRelation = true;
			if (right.getOperatorDataType() == OperatorDataType.RELATION) {
				setChildren(new LogicalOperator[] {left, right});
				setOperatorDataType(OperatorDataType.RELATION);
				leftChildIsRelation = true;
			} else if (right.getOperatorDataType() == OperatorDataType.WINDOWS) {
				//invert left and right so relation(s) are on the right.
				setChildren(new LogicalOperator[] {right, left});
				setOperatorDataType(OperatorDataType.WINDOWS);
			}
			else  {
				//invert left and right so relation(s) are on the right.
				setChildren(new LogicalOperator[] {right, left});
				setOperatorDataType(OperatorDataType.STREAM);
			}    	
		}    
	}

	/**
	 * Combines the attribute lists from the left and from the right.
	 * @throws SchemaMetadataException 
	 */
	private void getIncomingAttributes() throws SchemaMetadataException {
		attributes = new ArrayList<Attribute>(getInput(0).getAttributes());
		ArrayList <Attribute> right = 
			new ArrayList<Attribute>(getInput(1).getAttributes());
//		right.remove(new EvalTimeAttribute());
		attributes.addAll(right);
		expressions = new ArrayList <Expression>(attributes.size());
		for (int i = 0; i < attributes.size(); i++) {
			expressions.add(attributes.get(i));
		}
	}


	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */ 
	public List<Attribute> getAttributes() {
		return attributes;
	}

	/**
	 * {@inheritDoc}
	 * @throws SNEEConfigurationException 
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException, SNEEConfigurationException {

		boolean accepted = false;

		if (projectAttributes.size() > 0) {
			if (projectExpressions.size() == 0) {
				//remove unrequired attributes. No expressions to accept
				for (int i = 0; i < attributes.size(); ) {
					if (projectAttributes.contains(attributes.get(i)))
						i++;
					else {
						attributes.remove(i);
						expressions.remove(i);		
					}
				}
			}	
			else {
				expressions = projectExpressions;
				attributes = projectAttributes;
				accepted = true;
			}
		}
		ArrayList<Attribute> leftAttributes =  (ArrayList) getInput(0).getAttributes();
		List<Attribute> requiredLeftAttributes = (List<Attribute>) leftAttributes.clone();
		List<Attribute> unrequiredLeftAttributes = (List<Attribute>)leftAttributes.clone();
		
		ArrayList<Attribute> rightAttributes = (ArrayList) getInput(1).getAttributes();
		List<Attribute> requiredRightAttributes = (ArrayList<Attribute>)rightAttributes.clone();
		List<Attribute> unrequiredRightAttributes = (ArrayList<Attribute>)rightAttributes.clone();

		//Remove any attribute used by any output expression from the unrequired attribute Lists
		List<Attribute> requiredAttributes;
		for (int i = 0; i < expressions.size(); i++) {
			requiredAttributes = expressions.get(i).getRequiredAttributes();
			for (int j = 0; j < requiredAttributes.size(); j++) {
				if (unrequiredLeftAttributes.contains(requiredAttributes.get(j)))
					unrequiredLeftAttributes.remove(requiredAttributes.get(j));
				if (unrequiredRightAttributes.contains(requiredAttributes.get(j)))
					unrequiredRightAttributes.remove(requiredAttributes.get(j));
			}	
		}

		//Remove any attribute used by the predicate from the unrequired attribute Lists
		requiredAttributes = this.getPredicate().getRequiredAttributes();
		for (int j = 0; j < requiredAttributes.size(); j++) {
			if (unrequiredLeftAttributes.contains(requiredAttributes.get(j)))
				unrequiredLeftAttributes.remove(requiredAttributes.get(j));
			if (unrequiredRightAttributes.contains(requiredAttributes.get(j)))
				unrequiredRightAttributes.remove(requiredAttributes.get(j));
		}	

		requiredLeftAttributes.removeAll(unrequiredLeftAttributes);
		requiredRightAttributes.removeAll(unrequiredRightAttributes);

		getInput(0).pushProjectionDown(new  ArrayList<Expression>(), requiredLeftAttributes);
		getInput(1).pushProjectionDown(new  ArrayList<Expression>(), requiredRightAttributes);

		return accepted;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * STUB
	 * 
	 * @return False 
	 * @throws SNEEConfigurationException 
	 * @throws TypeMappingException 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 */
	public boolean pushSelectIntoLeafOp(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException,
	SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER/RETURN pushSelectIntoLeaf() return with false");
		}
		return false;
		/*
		 * Below code should work, but has not been tested and the rewriter now
		 * moves selects down and then tries only at the very bottom to move
		 * them in, so this is never called.
		 */
//		if (predicate.isJoinCondition()) {
//			if (logger.isDebugEnabled()) {
//				logger.debug("RETURN pushSelectIntoLeaf() with " +
//						"false (join condition)");
//			}
//			return false;
//		}
//		if (getInput(0).pushSelectIntoLeafOp(predicate) ||
//				getInput(1).pushSelectIntoLeafOp(predicate)) {
//			if (logger.isDebugEnabled()) {
//				logger.debug("RETURN pushSelectIntoLeaf() with true");
//			}
//			return true;
//		} else {
//			if (logger.isDebugEnabled()) {
//				logger.debug("RETURN pushSelectIntoLeaf() with false");
//			}
//			return false;
//		}
	}

	/** 
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
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
		int left = getInput(0).getCardinality(card);
		int right = getInput(1).getCardinality(card);
		if (getPredicate() instanceof NoPredicate) {
			return (left * right);
		}
		if ((card == CardinalityType.MAX) 
				|| (card == CardinalityType.PHYSICAL_MAX)) {
			return (left * right);
		} 
		if ((card == CardinalityType.AVERAGE) 
				|| (card == CardinalityType.MAX)) {
			return (left * right) / Constants.JOIN_PREDICATE_SELECTIVITY;
		}
		if (card == CardinalityType.MINIMUM) {
			return 0;
		} 
		throw new AssertionError("Unexpected CardinaliyType " + card);
	}

	/**
	 * Used to determine if the operator is Attribute sensitive.
	 *
	 * @return true`.
	 */
	public boolean isAttributeSensitive() {
		return true;
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
	public String toString() {
		return this.getText() + " [ " + getInput(0).toString() 
		+ ", " + getInput(1).toString() + " ]";
	}

//	/** {@inheritDoc} */
//	public JoinOperator shallowClone() {
//		JoinOperator clonedOp = new JoinOperator(this);
//		return clonedOp;
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(int tuples) {
//		return getOverheadTimeCost()
//		+ CostParameters.getCopyTuple() * tuples
//		+ CostParameters.getApplyPredicate() * tuples;
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, 
//			Site node, DAF daf) {
//		int left = getInputCardinality(card, node, daf, 0);
//		int right = getInputCardinality(card, node, daf, 0);
//		int tuples = left * right;
//		return getTimeCost(tuples);
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, 
//			int numberOfInstances) {
//		assert (numberOfInstances == 1);
//		int left = getInputCardinality(card, 0, 1);
//		int right = getInputCardinality(card, 0, 1);
//		int tuples = left * right;
//		return getTimeCost(tuples);
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(getOverheadTimeCost() + CostParameters.getCopyTuple());
//		AlphaBetaExpression tuples 
//		= this.getInputCardinality(card, node, daf, round, 0);
//		tuples.multiplyBy(getInputCardinality(card, node, daf, round, 1));
//		tuples.multiplyBy(CostParameters.getDoCalculation()
//				+ CostParameters.getApplyPredicate());
//		result.add(tuples);
//		return result;
//	}
	
//	/** {@inheritDoc} */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		int left = getInputCardinality(card, node, daf, 0);
//		int right = getInputCardinality(card, node, daf, 1);
//		if (getPredicate() instanceof NoPredicate) {
//			return (left * right);
//		}
//		if ((card == CardinalityType.MAX) 
//				|| (card == CardinalityType.PHYSICAL_MAX)) {
//			return (left * right);
//		} 
//		if ((card == CardinalityType.AVERAGE) 
//				|| (card == CardinalityType.MAX)) {
//			return (left * right) / Constants.JOIN_PREDICATE_SELECTIVITY;
//		}
//		if (card == CardinalityType.MINIMUM) {
//			return 0;
//		} 
//		throw new AssertionError("Unexpected CardinaliyType " + card);
//	}

	//	/** {@inheritDoc} */
	//	public AlphaBetaExpression getCardinality(CardinalityType card, 
	//			Site node, DAF daf, boolean round) {
	//		AlphaBetaExpression left 
	//		= getInputCardinality(card, node, daf, round, 0);
	//		AlphaBetaExpression right 
	//		= getInputCardinality(card, node, daf, round, 1);
	//		if (getPredicate() instanceof NoPredicate) {
	//			return AlphaBetaExpression.multiplyBy(left, right);
	//		}
	//		if ((card == CardinalityType.MAX) 
	//				|| (card == CardinalityType.PHYSICAL_MAX)) {
	//			return AlphaBetaExpression.multiplyBy(left, right);
	//		} 
	//		if ((card == CardinalityType.AVERAGE) 
	//				|| (card == CardinalityType.MAX)) {
	//			AlphaBetaExpression result 
	//			= AlphaBetaExpression.multiplyBy(left, right);
	//			result.divideBy(Constants.JOIN_PREDICATE_SELECTIVITY);
	//			return result;
	//		}
	//		if (card == CardinalityType.MINIMUM) {
	//			return new AlphaBetaExpression();
	//		} 
	//		throw new AssertionError("Unexpected CardinaliyType " + card);
	//	}

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		return true;
	}

	/** {@inheritDoc} */
	public boolean isRemoveable() {
		return false;
	}

	/** {@inheritDoc} */
	public List<Expression> getExpressions() {
		return expressions;
	}

	/** {@inheritDoc} */
	public boolean comesFromRightChild(String attrName) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * Join Operator places a relation source on the right if it is a mixed
	 * source join.
	 * 
	 * This method states whether the right child is a relation
	 * 
	 * @return true if the right child is a relation
	 */
	public boolean isRightChildRelation() {
		return rightChildIsRelation;
	}
	
	/**
	 * 
	 * This method states whether the left child is a relation
	 * 
	 * @return true if the left child is a relation
	 */
	public boolean isLeftChildRelation() {
		return leftChildIsRelation;
	}	
	
	/**
	 * Test to see if this is a join between two relations.
	 * 
	 * @return true if both sources are relations
	 */
	public boolean isRelationJoin() {
		if (rightChildIsRelation && leftChildIsRelation) { 
			return true;
		} else {
			return false;
		}
	}


	/**
	 * Method to set the mode in which the join operator is supposed to work
	 * @param mode
	 */
	public void setAlgorithm(String mode) {
		joinMode = mode;
	}
	
	/**
	 * Method to get the mode in which the operator is set to work
	 * @return
	 */
	public String getAlgorithm() {
		return joinMode;
	}	
	
	
	
	//Call to default methods in OperatorImplementation

	//	/** {@inheritDoc} */
	//	public int[] getSourceSites() {
	//		return super.defaultGetSourceSites();
	//	}

	//	/** {@inheritDoc} */    
	//	public int getOutputQueueCardinality(Site node, DAF daf) {
	//		return super.defaultGetOutputQueueCardinality(node, daf);
	//	}

	//	/** {@inheritDoc} */    
	//	public int getOutputQueueCardinality(int numberOfInstances) {
	//		return super.defaultGetOutputQueueCardinality(numberOfInstances);
	//	}

	//	/** {@inheritDoc} */    
	//	public int getDataMemoryCost(Site node, DAF daf) {
	//		return super.defaultGetDataMemoryCost(node, daf);
	//	}

}
