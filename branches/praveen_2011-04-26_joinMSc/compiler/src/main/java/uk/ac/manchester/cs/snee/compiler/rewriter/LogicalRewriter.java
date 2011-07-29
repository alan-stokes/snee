package uk.ac.manchester.cs.snee.compiler.rewriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.ExchangeOperator;
import uk.ac.manchester.cs.snee.operators.logical.InputOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class LogicalRewriter {

	Logger logger = Logger.getLogger(this.getClass().getName());
	
	private AttributeType _boolType;
	
	public LogicalRewriter (MetadataManager metadata) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER LogicalRewriter()");
		
		if (logger.isTraceEnabled()) {
			logger.trace("#extents=" + metadata.getExtentNames().size());
		}
		_boolType = metadata.getTypes().getType("boolean");
		if (logger.isDebugEnabled())
			logger.debug("RETURN LogicalRewriter()");
	}
	
	public LAF doLogicalRewriting(LAF laf) throws SNEEConfigurationException,
	OptimizationException, SchemaMetadataException, AssertionError, 
	TypeMappingException {
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
	    if (SNEEProperties.getBoolSetting(
	    		SNEEPropertyNames.LOGICAL_REWRITER_REMOVE_UNREQUIRED_OPS)) {
			if (logger.isInfoEnabled()) {
				logger.info("Removing unrequired operators");
			}
	    	removeUnrequiredOperators(laf);
	    	removeRStream(laf);
	    }
	    //Praveen COde Begins
		if (logger.isInfoEnabled()) {
			logger.info("Combine selection into join");
		}
	    combineSelectAndJoin(laf);
		if (logger.isInfoEnabled()) {
			logger.info("Insert Exchange Operators");
		}
		insertExchangeOperators(laf);
	    if (SNEEProperties.getBoolSetting(SNEEPropertyNames.
	    		LOGICAL_REWRITER_INSERT_VALVE_OPS)) {
			if (logger.isInfoEnabled()) {
				logger.info("Inserving Valve Operators");
			}
	    	insertValveOperator(laf);
	    }
	    
	    //Praveen Code Ends
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
	protected void pushSelectionDown(LAF laf) 
	throws OptimizationException, SchemaMetadataException, AssertionError, 
	TypeMappingException, SNEEConfigurationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER pushSelectionDown() with " + laf);
		}
		moveSelectClauseDown(laf);
		combineSelections(laf);
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
							swapOperators(op, childOp);
						} else {
							reachedBottom = true;
						}
					} else if (childOp instanceof ProjectOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Move SELECT below PROJECT");
						}
						swapOperators(op, childOp);
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
							swapOperators(op, childOp);
							//					} else if (!predicate.isJoinCondition() && 
							//							!childPredicate.isJoinCondition()) {
							//						// Combine if both for same extent
						} else {
							reachedBottom = true;
						}
					} else if (childOp instanceof JoinOperator) {
						if (logger.isTraceEnabled()) {
							logger.trace("Attempt to move SELECT below JOIN");
						}
						SelectOperator selectOp = (SelectOperator) op;
						JoinOperator joinOp = (JoinOperator) childOp;
						Expression predicate = selectOp.getPredicate();
						if (predicate.isJoinCondition()) {
							//FIXME: Check if we can move below this join instance
							if (logger.isTraceEnabled()) {
								logger.trace("Attempt to move JOIN-SELECT below JOIN");
							}
							reachedBottom = true;
						} else {
							if (logger.isTraceEnabled()) {
								logger.trace("Move SELECT below JOIN");
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
								// Connect parent of select op to join op 
								LogicalOperator parentOp = op.getParent();
								parentOp.setInput(childOp, 0);
								// Get left child
								LogicalOperator grandChildOp = childOp.getInput(0);
								// Set left child
								childOp.setInput(op, 0);
								op.setInput(grandChildOp, 0);
								op.setOutput(childOp, 0);
								childOp.setOutput(parentOp, 0);
								grandChildOp.setOutput(op, 0);
							} else {
								if (logger.isTraceEnabled()) {
									logger.trace("Move SELECT below right child");
								}																
								// Connect parent of select op to join op 
								LogicalOperator parentOp = op.getParent();
								parentOp.setInput(childOp, 0);
								LogicalOperator grandChildOp = childOp.getInput(1);
								childOp.setInput(op, 1);
								op.setInput(grandChildOp, 0);
								op.setOutput(childOp, 0);
								childOp.setOutput(parentOp, 0);
								grandChildOp.setOutput(op, 0);
							}
						}
					}
				}
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN moveSelectClauseDown()");
		}
	}

	private void swapOperators(LogicalOperator op, LogicalOperator childOp) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER swapOperators() with " +
					op.toString() + " and " + childOp.toString());
		}
		LogicalOperator parentOp = op.getParent();
		if (parentOp instanceof JoinOperator) {
			if (logger.isTraceEnabled()) {
				logger.trace("Parent operator is a JOIN. Need to take care");
			}
			if (op == parentOp.getInput(0)) {
				// Left child of join
				LogicalOperator grandChildOp = childOp.getInput(0);
				parentOp.setInput(childOp, 0);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			} else {
				//Right child of join
				LogicalOperator grandChildOp = childOp.getInput(0);
				parentOp.setInput(childOp, 1);
				childOp.setInput(op, 0);
				op.setInput(grandChildOp, 0);
				op.setOutput(childOp, 0);
				childOp.setOutput(parentOp, 0);
				grandChildOp.setOutput(op, 0);
			}
		} else {
			LogicalOperator grandChildOp = childOp.getInput(0);
			parentOp.setInput(childOp, 0);
			childOp.setInput(op, 0);
			op.setInput(grandChildOp, 0);
			op.setOutput(childOp, 0);
			childOp.setOutput(parentOp, 0);
			grandChildOp.setOutput(op, 0);
		}
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
	 * This method accepts the laf tree. It does a post order traversal
	 * on it. During the traversal, if nodes are found whose children
	 * are Push operators, then a valve operator is inserted between the parent
	 * and the child to avoid the overwhelming of the parent operator with the
	 * incoming data
	 * 
	 * @param laf
	 */
//	private void insertValveOperator(LAF laf) {
//		Iterator<LogicalOperator> opIter = laf
//				.operatorIterator(TraversalOrder.POST_ORDER);
//		LogicalOperator childOperator;
//		LogicalOperator op;
//		Iterator<LogicalOperator> iter;
//		while (opIter.hasNext()) {
//			op = opIter.next();
//			iter = op.childOperatorIterator();
//			if (!(op instanceof WindowOperator) && iter.hasNext()) {
//				do {
//					childOperator = iter.next();
//					SourceType opSourceType = childOperator
//							.getOperatorSourceType();
//					if ((opSourceType == SourceType.PULL_STREAM_SERVICE
//							|| opSourceType == SourceType.UDP_SOURCE 
//							|| opSourceType == SourceType.PUSH_STREAM_SERVICE)) {
//
//						laf.getOperatorTree().insertNode(childOperator, op,
//								new ValveOperator(childOperator, _boolType));
//
//					}
//				} while (iter.hasNext());
//
//			}
//
//		}
//
//	}
	
	/**
	 * This method accepts the laf tree. It does a post order traversal
	 * on it. During the traversal, all the join operators that fall outside
	 * the sensor network are buffered with a valve operator
	 * 
	 * @param laf
	 */
	private void insertValveOperator(LAF laf) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER insertValveOperator()");
		}
		Iterator<LogicalOperator> opIter = laf
				.operatorIterator(TraversalOrder.PRE_ORDER);
		LogicalOperator childOperator;
		LogicalOperator op;
		Iterator<LogicalOperator> iter;
		while (opIter.hasNext()) {
			op = opIter.next();
			if (op instanceof JoinOperator
					&& SourceType.SENSOR_NETWORK != op.getOperatorSourceType()) {
				iter = op.childOperatorIterator();
				if (iter.hasNext()) {
					do {
						childOperator = iter.next();

						laf.getOperatorTree().insertNode(childOperator, op,
								new ValveOperator(childOperator, _boolType));

					} while (iter.hasNext());
				}
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER insertValveOperator()");
		}
	}

	/**
	 * This method inserts exchange operators between operators, where a boundary
	 * is found between a sensor network and a non-sensor network.
	 * 
	 * @param laf
	 */
	private void insertExchangeOperators(LAF laf) {

		Iterator<LogicalOperator> opIter = laf
				.operatorIterator(TraversalOrder.POST_ORDER);
		LogicalOperator childOperator;
		LogicalOperator op;
		Iterator<LogicalOperator> iter;
		while (opIter.hasNext()) {
			op = opIter.next();
			iter = op.childOperatorIterator();
			if (!(op instanceof WindowOperator) && iter.hasNext()) {
				do {
					childOperator = iter.next();
					SourceType opSourceType = childOperator
							.getOperatorSourceType();
					if (opSourceType == SourceType.SENSOR_NETWORK 
							&& op.getOperatorSourceType() != opSourceType) {

						laf.getOperatorTree().insertNode(childOperator, op,
								new ExchangeOperator(childOperator, _boolType));

					}
				} while (iter.hasNext());

			}

		}
		
	}

	/** 
     * Removes unrequired operators.
     * Removes operators that report they are removeable.
     * Removal operators are ones that have the same physical input as output.
     * @param laf The query operator tree in logical-algebraic form.
     */
 	protected void removeUnrequiredOperators(final LAF laf) 
 	throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER removeUnrequiredOperators() laf="+laf.getID());
		final Iterator<LogicalOperator> opIter = laf
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final LogicalOperator op = opIter.next();
			if (op.isRemoveable()) {
			    logger.trace("Removing node " + op.getText());
			    laf.removeOperator(op);
			}
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
