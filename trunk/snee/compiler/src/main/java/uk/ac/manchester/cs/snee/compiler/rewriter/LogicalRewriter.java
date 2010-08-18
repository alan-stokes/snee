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
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;

public class LogicalRewriter {

	
	Logger logger = Logger.getLogger(this.getClass().getName());
	
	public LogicalRewriter () {
		if (logger.isDebugEnabled())
			logger.debug("ENTER LogicalRewriter()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN LogicalRewriter()");
	}
	
	public LAF doLogicalRewriting(LAF laf) throws SNEEConfigurationException,
	OptimizationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doLogicalRewriting() laf="+laf.getName());
		String lafName = laf.getName();
		String newLafName = lafName.replace("LAF", "LAF'");
		laf.setName(newLafName);
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
		if (logger.isDebugEnabled())
			logger.debug("RETURN doLogicalRewriting()");
		return laf;
	}
	
	
    /** 
     * Removes unrequired operators.
     * Removes operators that report they are removeable.
     * Removal operators are ones that have the same physical input as output.
     * @param laf The query operator tree in logical-algebraic form.
     */
 	private void removeUnrequiredOperators(final LAF laf) throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER removeUnrequiredOperators() laf="+laf.getName());
		final Iterator<LogicalOperator> opIter = laf
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final LogicalOperator op = opIter.next();
			if (op.isRemoveable()) {
			    logger.trace("Removing node " + op.getText());
			    laf.removeNode(op);
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
			logger.debug("ENTER removeRStream() laf="+laf.getName());
		final Iterator<LogicalOperator> opIter = laf
			.operatorIterator(TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
		    final LogicalOperator op = opIter.next();
			if (op instanceof RStreamOperator) {
			    logger.trace("Removing node " + op.getText());
			    laf.removeNode(op);
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
			logger.debug("ENTER pushProjectionDown() laf="+laf.getName());
		final LogicalOperator op 
			= (LogicalOperator) laf.getRootOperator();
		op.pushProjectionDown(new ArrayList<Expression>(),
				new ArrayList<Attribute>());
		if (logger.isTraceEnabled())
			logger.debug("RETURN pushProjectionDown()");
    }
    
}
