package uk.ac.manchester.cs.snee.compiler.rewriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.InputOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.OperatorDataType;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class LogicalRewriter {

	Logger logger = Logger.getLogger(this.getClass().getName());
		
	public LAF doLogicalRewriting(LAF laf) 
	throws SNEEConfigurationException, OptimizationException, SchemaMetadataException, 
	AssertionError, TypeMappingException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doLogicalRewriting() laf="+laf.getID());
		String lafName = laf.getID();
		String newLafName = lafName.replace("LAF", "LAF'");
		laf.setID(newLafName);
		logger.trace("renamed "+lafName+" to "+newLafName);
		if (logger.isInfoEnabled()) {
			logger.info("Pushing selections down");
		}
		pushSelectionDown(laf);
		if (logger.isInfoEnabled()) {
			logger.info("Pushing projections down");
		}
		pushProjectionDown(laf);
		removeUnrequiredOperators(laf);
		if (logger.isInfoEnabled()) {
			logger.info("Combine selection into join");
		}
	    combineSelectAndJoin(laf);
		if (logger.isDebugEnabled())
			logger.debug("RETURN doLogicalRewriting()");
		return laf;
	}
	
	/**
	 * This method traverses the LAF moving selection operators to the lowest 
	 * point allowed.
	 * 
	 * @param laf
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws TypeMappingException 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 */
	private void pushSelectionDown(LAF laf) 
	throws OptimizationException, SchemaMetadataException, AssertionError, 
	TypeMappingException, SNEEConfigurationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER pushSelectionDown() with " + laf);
		}
		moveSelectClauseDown(laf);
		// Following lines useful for debugging
//		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
//			String lafName = laf.getID();
//			String newLafName = lafName.replace("LAF", "LAF'-push");
//			laf.setID(newLafName);
//			new LAFUtils(laf).generateGraphImage();
//		}

		combineSelections(laf);
//		// Following lines useful for debugging
//		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
//			String lafName = laf.getID();
//			String newLafName = lafName.replace("LAF'-push", "LAF'-combine");
//			laf.setID(newLafName);
//			new LAFUtils(laf).generateGraphImage();
//			//Reset LAF' name
//			lafName = laf.getID();
//			newLafName = lafName.replace("LAF'-combine", "LAF'");
//			laf.setID(newLafName);
//		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN pushSelectionDown()");
		}
	}

	/**
	 * Moves a selection condition, not a join condition, as far down the LAF
	 * as possible.
	 * 
	 * Note, a selection condition can be moved below a time window but not a
	 * row based window.
	 * 
	 * @param laf
	 * @throws OptimizationException
	 * @throws SNEEConfigurationException 
	 * @throws TypeMappingException 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 */
	private void moveSelectClauseDown(LAF laf) 
	throws OptimizationException, SchemaMetadataException, AssertionError, 
	TypeMappingException, SNEEConfigurationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER moveSelectClauseDown()");
		}
		Iterator<LogicalOperator> opIter = laf.operatorIterator(
				TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
			if (op instanceof SelectOperator) {
				boolean reachedBottom = false;
				while (!reachedBottom) {
					LogicalOperator childOp = op.getInput(0);
					if (childOp instanceof InputOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Try to push SELECT into INPUT Op");
						}
						if (childOp.pushSelectIntoLeafOp(op.getPredicate())) {
							if (logger.isTraceEnabled()) {
								logger.trace("INPUT op and SELECT op combined. " +
										"Remove SELECT op.");
							}
							laf.removeOperator(op);
						}
						reachedBottom = true;
					} else if (childOp instanceof WindowOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Attempt to move SELECT below WINDOW");
						}
						WindowOperator winOp = (WindowOperator) childOp;
						if (winOp.isTimeScope()) {
							swapOperators(laf, op, childOp);
							//Change the data type associated with the select operator
							op.setOperatorDataType(OperatorDataType.STREAM);
						} else {
							reachedBottom = true;
						}
					} else if (childOp instanceof SelectOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Attempt to move SELECT below SELECT");
						}
						SelectOperator selectOp = (SelectOperator) op;
						SelectOperator childSelectOp = (SelectOperator) childOp;
						Expression predicate = selectOp.getPredicate();
						Expression childPredicate = childSelectOp.getPredicate();
						if (!predicate.isJoinCondition() &&
								childPredicate.isJoinCondition()) {
							if (logger.isTraceEnabled()) {
								logger.trace("Move SELECT below JOIN-SELECT");
							}						
							// Move selection below join condition
							swapOperators(laf, op, childOp);
						} else {
							if (logger.isTraceEnabled()) {
								logger.trace("Move JOIN-SELECT below JOIN-SELECT");
							}													
							swapOperators(laf, op, childOp);
						}
					} else if (childOp instanceof JoinOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Attempt to move SELECT below JOIN");
						}
						SelectOperator selectOp = (SelectOperator) op;
						JoinOperator joinOp = (JoinOperator) childOp;
						Expression predicate = selectOp.getPredicate();
						if (predicate.isJoinCondition()) {
							reachedBottom = moveJoinSelectConditionBelowJoin(
									op, joinOp, predicate);
						} else {
							moveSelectBelowJoin(op, joinOp, predicate);
						}
					} else if (childOp instanceof AggregationOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Attempt to move SELECT below AGGREGATION " + 
									childOp.getParamStr());
						}
						reachedBottom = canMoveSelectBelowAggregation((SelectOperator) op, 
								(AggregationOperator) childOp);
						if (!reachedBottom) {
							swapOperators(laf, op, childOp);
						}
					} else { //if (childOp instanceof ProjectOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Move SELECT below " + 
									childOp.getOperatorName());
						}
						swapOperators(laf, op, childOp);
					}
				}
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveSelectClauseDown()");
		}
	}

	private boolean canMoveSelectBelowAggregation(SelectOperator selectOp,
			AggregationOperator aggOp) throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER moveSelectBelowAggregation() " +
					"\n\t" + selectOp + "\n\t" + aggOp);
		}
		boolean reachedBottom = false;
		List<Attribute> selectRequiredAttributes = selectOp.getPredicate().getRequiredAttributes();
		List<AggregationExpression> aggregates = aggOp.getAggregates();
		try {
			for (AggregationExpression aggExp : aggregates) {
				Attribute attr = aggExp.toAttribute();
				if (selectRequiredAttributes.contains(attr)) {
					reachedBottom = true;
					break;
				}
			}
		} catch (SchemaMetadataException e) {
			logger.warn(e);
			throw new OptimizationException(null, e);
		} catch (TypeMappingException e) {
			logger.warn(e);
			throw new OptimizationException(null, e);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveSelectBelowAggregation() with " + reachedBottom);
		}
		return reachedBottom;
	}

	private boolean moveJoinSelectConditionBelowJoin(LogicalOperator op,
			JoinOperator joinOp, Expression predicate) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER moveJoinSelectConditionBelowJoin() with" +
					"\n\tSELECT op " + op + "\n\tJOIN op " + joinOp);
		}
		Set<String> extents = getExtents(predicate.getRequiredAttributes());
		Set<String> extentsLeft = getExtents(joinOp.getInput(0).getAttributes());
		Set<String> extentsRight = getExtents(joinOp.getInput(1).getAttributes());
		boolean foundLeft = false;
		boolean foundRight = false;
		for (String extentName : extents) {
			if (extentsLeft.contains(extentName)) {
				foundLeft = true;
			}
			if (extentsRight.contains(extentName)) {
				foundRight = true;
			}
		}
		boolean reachedBottom;
		if (foundLeft && !foundRight) {
			//Swap with left join child
			swapWithJoinChildOperator(op, joinOp, 0);
			reachedBottom = false;
		} else if (!foundLeft && foundRight) {
			//Swap with right join child
			swapWithJoinChildOperator(op, joinOp, 1);
			reachedBottom = false;
		} else if (foundLeft && foundRight) {
			reachedBottom = true;
			// Combining with join op done later in the logical rewriter
		} else {
			String message = "Attributes required for select operator condition " +
					"could not be found on left or right child of join operator.";
			logger.warn(message + "\n\tSELECT OP: " + op + "\n\tJOIN OP: " + joinOp);
			throw new OptimizationException(message);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveJoinSelectConditionBelowJoin() " +
					"reachedBottom=" + reachedBottom);
		}
		return reachedBottom;
	}

	/**
	 * Retrieve the extents required to populate a list of attributes, 
	 * ignoring the system extent
	 * @param attributes to find the extents involved
	 * @return set of extent names
	 */
	private Set<String> getExtents(List<Attribute> attributes) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getExtents() with " + attributes);
		}
		Set<String> extents = new HashSet<String>();
		for (Attribute attr : attributes) {
			String extentName = attr.getExtentName();
			// Ignore system extent
			if (!extentName.equalsIgnoreCase("system")) {
				extents.add(extentName);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getExtents() #extents=" + extents.size());
		}
		return extents;
	}

	private void moveSelectBelowJoin(LogicalOperator op,
			JoinOperator joinOp, Expression predicate) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER moveSelectBelowJoin() with" +
					"\n\tSELECT op " + op + "\n\tJOIN op " + joinOp);
		}
		String extentName = predicate.
			getRequiredAttributes().get(0).getExtentName();
		LogicalOperator leftOp = joinOp.getInput(0);
		List<Attribute> leftAttrs = leftOp.getAttributes();
		boolean foundLeft = false;
		for (Attribute attr : leftAttrs) {
			String leftExtentName = attr.getExtentName();
			if (leftExtentName.equalsIgnoreCase(extentName)) {
				foundLeft = true;
				break;
			}
		}
		if (foundLeft) {
			if (logger.isTraceEnabled()) {
				logger.trace("Move SELECT below left child");
			}	
			swapWithJoinChildOperator(op, joinOp, 0);
		} else {
			if (logger.isTraceEnabled()) {
				logger.trace("Move SELECT below right child");
			}	
			swapWithJoinChildOperator(op, joinOp, 1);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveSelectBelowJoin()");
		}
	}

	/**
	 * Swaps the given operator below the left (0) or right (1) side of a join
	 * @param op operator to move below the join
	 * @param joinOp join operator 
	 * @param inputPos specifies if it is the left (0) or right (1) side of the 
	 * join that needs to be moved below
	 */
	private void swapWithJoinChildOperator(LogicalOperator op, 
			LogicalOperator joinOp, int inputPos) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER swapWithJoinChildOperator() with " +
					op.toString() + " and " + joinOp.toString() + 
					" position=" + inputPos);
		}
		LogicalOperator parentOp = op.getParent();
		parentOp.setInput(joinOp, 0);
		LogicalOperator grandChildOp = joinOp.getInput(inputPos);
		joinOp.setInput(op, inputPos);
		op.setInput(grandChildOp, 0);
		op.setOutput(joinOp, 0);
		joinOp.setOutput(parentOp, 0);
		grandChildOp.setOutput(op, 0);		
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN swapWithJoinChildOperator()");
		}		
	}
	
	private void swapOperators(LAF laf, LogicalOperator op, LogicalOperator childOp) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER swapOperators() with " +
					op.toString() + " and " + childOp.toString());
		}
		LogicalOperator parentOp = op.getParent();
		LogicalOperator grandChildOp = childOp.getInput(0);
		if (parentOp instanceof JoinOperator) {
			if (logger.isTraceEnabled()) {
				logger.trace("Parent operator is a JOIN. Need to take care");
			}
			
			if (op == parentOp.getInput(0)) {
				// Left child of join
				parentOp.setInput(childOp, 0);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			} else {
				//Right child of join
				parentOp.setInput(childOp, 1);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			}
		} else {
			parentOp.setInput(childOp, 0);
			childOp.setInput(op, 0);
			op.setInput(grandChildOp, 0);
			op.setOutput(childOp, 0);
			childOp.setOutput(parentOp, 0);
			grandChildOp.setOutput(op, 0);
		}
		laf.getOperatorTree().removeEdge(childOp, op);
		laf.getOperatorTree().removeEdge(op, parentOp);
		laf.getOperatorTree().removeEdge(grandChildOp, childOp);
		laf.getOperatorTree().addEdge(childOp, parentOp);
		laf.getOperatorTree().addEdge(op, childOp);
		laf.getOperatorTree().addEdge(grandChildOp, op);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN swapOperators()");
		}
	}
	
	/**
	 * This method traverses the LAF moving selection operators to the lowest 
	 * point allowed.
	 * 
	 * @param laf
	 * @throws TypeMappingException 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 */
	private void combineSelections(LAF laf) 
	throws SchemaMetadataException, AssertionError, TypeMappingException,
	OptimizationException, SNEEConfigurationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER combineSelections() with " + laf);
		}
		Iterator<LogicalOperator> opIter = laf.operatorIterator(
				TraversalOrder.POST_ORDER);
		// Selects can be combined after being pushed down as far as possible
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
			if (op instanceof SelectOperator) {
				LogicalOperator parentOp = op.getParent();
				if (parentOp instanceof SelectOperator) {
					((SelectOperator) parentOp).combineSelects(op.getPredicate());
					laf.removeOperator(op);
				}
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN combineSelections()");
		}
	}

	/**
	 * In case there are predicates defined for a selection which is immediately
	 * preceded by a join condition, the following method combines both the operations
	 * into a single join operator with the appropriate predicates set into the join
	 * condition
	 * 
	 * @param laf
	 * @throws SchemaMetadataException
	 * @throws AssertionError
	 * @throws TypeMappingException
	 * @throws OptimizationException
	 */
	private void combineSelectAndJoin(LAF laf) throws SchemaMetadataException,
			AssertionError, TypeMappingException, OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER combineSelectAndJoin() with LAF " + laf);
		}
		Iterator<LogicalOperator> opIter = laf
				.operatorIterator(TraversalOrder.POST_ORDER);
		LogicalOperator childOperator;
		LogicalOperator op;		
		while (opIter.hasNext()) {
			op = opIter.next();
			if (op instanceof SelectOperator) {
				childOperator = op.getInput(0);
				if (!(op.getPredicate() instanceof NoPredicate)
						&& childOperator instanceof JoinOperator) {
					childOperator.setPredicate(op.getPredicate());
					logger.trace("Removing node " + op.getText());
					laf.removeOperator(op);
				}
			}		

		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN combineSelectAndJoin()");
		}
	}
	
    /** 
     * Removes unrequired operators.
     * Removes operators that report they are removeable.
     * Removal operators are ones that have the same physical input as output.
     * @param laf The query operator tree in logical-algebraic form.
     */
 	private void removeUnrequiredOperators(final LAF laf) 
 	throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER removeUnrequiredOperators() laf="+laf.getID());
		/*
		 * Test if there is a setting for removing unrequired operators,
		 * Default to true
		 */
		boolean boolSetting;
		try {
			boolSetting = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.LOGICAL_REWRITER_REMOVE_UNREQUIRED_OPS);
		} catch (SNEEConfigurationException e) {
			boolSetting = true;
		}
		if (boolSetting) {
			if (logger.isInfoEnabled()) {
				logger.info("Removing unrequired operators");
			}
			final Iterator<LogicalOperator> opIter = laf
			.operatorIterator(TraversalOrder.POST_ORDER);
			while (opIter.hasNext()) {
				final LogicalOperator op = opIter.next();
				if (op.isRemoveable()) {
					logger.trace("Removing node " + op.getText());
					laf.removeOperator(op);
				}
			}
			removeRStream(laf);
		}
	    if (logger.isTraceEnabled())
			logger.debug("RETURN removeUnrequiredOperators()");
    }	
	
 	/** 
 	 * Removes all RStream operators.
     * @param laf The Logical algebraic form.
 	 * @throws OptimizationException 
 	 */
    private void removeRStream(final LAF laf) throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER removeRStream() laf="+laf.getID());
		final Iterator<LogicalOperator> opIter = laf
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final LogicalOperator op = opIter.next();
			if (op instanceof RStreamOperator) {
			    logger.trace("Removing node " + op.getText());
			    laf.removeOperator(op);
			}
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN removeRStream()");
    }
 	
    /**
     * Pushes projections down into other operators.
     * @param laf The Logical algebraic form.
     * @throws OptimizationException 
     * @throws SNEEConfigurationException 
     */
    private void pushProjectionDown(final LAF laf)     
    throws OptimizationException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER pushProjectionDown() laf="+laf.getID());
		final LogicalOperator op 
			= (LogicalOperator) laf.getRootOperator();
		op.pushProjectionDown(new ArrayList<Expression>(),
				new ArrayList<Attribute>());
		if (logger.isTraceEnabled())
			logger.debug("RETURN pushProjectionDown()");
    }
    
}
