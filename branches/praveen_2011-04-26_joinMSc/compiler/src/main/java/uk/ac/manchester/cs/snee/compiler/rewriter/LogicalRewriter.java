package uk.ac.manchester.cs.snee.compiler.rewriter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.ExchangeOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class LogicalRewriter {

	
	Logger logger = Logger.getLogger(this.getClass().getName());
	
	private AttributeType _boolType;
	
	public LogicalRewriter (MetadataManager metadata) throws TypeMappingException, SchemaMetadataException {
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
	OptimizationException, SchemaMetadataException, AssertionError, TypeMappingException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doLogicalRewriting() laf="+laf.getID());
		String lafName = laf.getID();
		String newLafName = lafName.replace("LAF", "LAF'");
		laf.setID(newLafName);
		logger.trace("renamed "+lafName+" to "+newLafName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.
	    LOGICAL_REWRITER_PUSH_PROJECT_DOWN)) {
	    	pushProjectionDown(laf);
	    }
	    if (SNEEProperties.getBoolSetting(SNEEPropertyNames.
	    LOGICAL_REWRITER_REMOVE_UNREQUIRED_OPS)) {
	    	removeUnrequiredOperators(laf);
	    	removeRStream(laf);
	    }
	    //Praveen COde Begins
	    boolean combineSelectandJoin = true;//need to convert to SNEE property if allowed
	    if (combineSelectandJoin) {
	    	combineSelectandJoin(laf);
	    }
	    insertExchangeOperators(laf);
	    insertValveOperator(laf);
	    //Praveen Code Ends
		if (logger.isDebugEnabled())
			logger.debug("RETURN doLogicalRewriting()");
		return laf;
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
	private void combineSelectandJoin(LAF laf) throws SchemaMetadataException,
			AssertionError, TypeMappingException, OptimizationException {
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
	private void insertValveOperator(LAF laf) {
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
					if ((opSourceType == SourceType.PULL_STREAM_SERVICE
							|| opSourceType == SourceType.UDP_SOURCE 
							|| opSourceType == SourceType.PUSH_STREAM_SERVICE)) {

						laf.getOperatorTree().insertNode(childOperator, op,
								new ValveOperator(childOperator, _boolType));

					}
				} while (iter.hasNext());

			}

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
 	private void removeUnrequiredOperators(final LAF laf) throws OptimizationException {
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
     */
    private void pushProjectionDown(final LAF laf)     
    throws OptimizationException {
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
