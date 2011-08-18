package uk.ac.manchester.cs.snee.compiler.rewriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.InputOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
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
		//FIXME: Issue of combining multiple copies of acquires on the same extent!!!
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
//			// Following lines useful for debugging
//		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
//			String lafName = laf.getID();
//			String newLafName = lafName.replace("LAF", "LAF'-push");
//			laf.setID(newLafName);
//			new LAFUtils(laf).generateGraphImage();
//		}

		combineSelections(laf);
//			// Following lines useful for debugging
//		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
//			String lafName = laf.getID();
//			String newLafName = lafName.replace("LAF'-push", "LAF'-combine");
//			laf.setID(newLafName);
//			new LAFUtils(laf).generateGraphImage();
//		}
//		// Following lines useful for debugging
//		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
//			String lafName = laf.getID();
//			String newLafName = lafName.replace("LAF'-combine", "LAF'-pushFinal");
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
	 * Note, a selection condition can be move below a time window but not a
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
		// If select contains attribute(s) from one source, can be pushed below join
		// Select can be pushed below time window, not a tuple window though
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

	private boolean moveJoinSelectConditionBelowJoin(LogicalOperator op,
			JoinOperator joinOp, Expression predicate) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER moveJoinSelectConditionBelowJoin() with" +
					"\n\tSELECT op " + op + "\n\tJOIN op " + joinOp);
		}
		boolean reachedBottom = true;
		String extent1 = 
			predicate.getRequiredAttributes().get(0).getExtentName();
		String extent2 = 
			predicate.getRequiredAttributes().get(1).getExtentName();
		List<Node> inputsOperators = joinOp.getInputsList();
		for (int i = 0; i < inputsOperators.size(); i++) {
			LogicalOperator joinChildOp = 
				(LogicalOperator) inputsOperators.get(i);
			List<Attribute> attributes = joinChildOp.getAttributes();
			boolean found1 = false;
			boolean found2 = false;
			for (Attribute attr : attributes) {
				if (attr.getExtentName().equalsIgnoreCase(extent1)) {
					found1 = true;
				}
				if (attr.getExtentName().equalsIgnoreCase(extent2)) {
					found2 = true;
				}
			}
			if (found1 && found2) {
				//Need to swap with join child
				swapWithJoinChildOperator(op, joinOp, i);
				reachedBottom = false;
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveJoinSelectConditionBelowJoin() " +
					"reachedBottom=" + reachedBottom);
		}
		return reachedBottom;
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
	
	private void swapOperators(LAF laf, LogicalOperator op, LogicalOperator childOp) throws OptimizationException {
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
				//LogicalOperator grandChildOp = childOp.getInput(0);
				parentOp.setInput(childOp, 0);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			} else {
				//Right child of join
				//LogicalOperator grandChildOp = childOp.getInput(0);
				parentOp.setInput(childOp, 1);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			}
		} else {
			//LogicalOperator grandChildOp = childOp.getInput(0);			
			//laf.removeOperator(op);
			//laf.getOperatorTree().insertNode(grandChildOp, childOp,
			//		op);
			//LogicalOperator grandChildOp = childOp.getInput(0);
			//laf.getOperatorTree().removeEdge(childOp, op);
			//laf.getOperatorTree().removeEdge(op, parentOp);
			//laf.getOperatorTree().removeEdge(grandChildOp, childOp);
			parentOp.setInput(childOp, 0);
			//laf.getOperatorTree().addEdge(childOp, parentOp);
			childOp.setInput(op, 0);
			//laf.getOperatorTree().addEdge(op, childOp);
			op.setInput(grandChildOp, 0);
			//laf.getOperatorTree().addEdge(grandChildOp, op);
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
