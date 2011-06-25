/**
 * 
 */
package uk.ac.manchester.cs.snee.compiler.algorithm;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;

/**
 * @author Praveen
 * 
 */
public class AlgorithmSelector {
	Logger logger = Logger.getLogger(this.getClass().getName());

	public AlgorithmSelector() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AlgorithmSelector()");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN AlgorithmSelector()");
		}
	}

	/**
	 * This method initiates the selection of algorithms for the different 
	 * operators present in the dlaf. This renames the DLAF to DLAF'.
	 * 
	 * @param dlaf
	 * @return
	 */
	public DLAF doAlgorithmSelection(DLAF dlaf) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER doAlgorithmSelection() laf=" + dlaf.getID());
		}
		String dlafName = dlaf.getID();
		String newDLafName = dlafName.replace("DLAF", "DLAF'");
		dlaf.setID(newDLafName);
		logger.trace("renamed " + dlafName + " to " + newDLafName);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN doAlgorithmSelection()");
		}
		replaceWithAlgorithms(dlaf);
		return dlaf;

	}

	/**
	 * This method iterates the dlaf and assigns the specific
	 * operator with its associated algorithms, by routing the
	 * calls to appropriate methods
	 * 
	 * @param dlaf
	 */
	private void replaceWithAlgorithms(DLAF dlaf) {
		Iterator<LogicalOperator> opIter = dlaf
				.operatorIterator(TraversalOrder.POST_ORDER);
		LogicalOperator op;
		while (opIter.hasNext()) {
			op = opIter.next();
			if (op instanceof JoinOperator) {
				setJoinOperatorAlgorithm((JoinOperator)op);
			} else if (op instanceof ValveOperator) {
				setValveOperatorAlgorithm((ValveOperator)op);
			}
		}

	}

	/**
	 * Set the algorithm with which the valve operator would work
	 * 
	 * @param valveOperator
	 */
	private void setValveOperatorAlgorithm(ValveOperator valveOperator) {
		//TODO set it via property
		double thresholdRate = 2.0;
		if (valveOperator.getInput(0).getSourceRate() > thresholdRate) {
			valveOperator.setPushBasedOperator(false);
			valveOperator.setAlgorithm(ValveOperator.TUPLE_DROP_MODE);
			//TODO set 
			valveOperator.getParent().setGetDataByPullModeOperator(true);
		} else {
			
		}
	}

	/**
	 * Set the algorithm with which the join operator will work
	 * 
	 * @param joinOperator
	 */
	private void setJoinOperatorAlgorithm(JoinOperator joinOperator) {
		if (joinOperator.getPredicate() instanceof NoPredicate) {
			//joinOperatorImpl = new JoinOperatorImpl(op, qid);
			joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
		} else {
			Expression predicate = joinOperator.getPredicate();
			// check if all the expressions in the predicate are of
			// equality type and only in such cases perform HashJoin
			if (isEqualityTypePred(predicate)) {
				//joinOperatorImpl = new HashJoinOperatorImpl(op, qid);
				joinOperator.setAlgorithm(JoinOperator.SHJ_MODE);
			} else {
				//joinOperatorImpl = new JoinOperatorImpl(op, qid);
				joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
			}
		}

	}

	/**
	 * Method that checks if all the join condition within the
	 * predicate are of equality type
	 * 
	 * @param predicate
	 * @return
	 */
	private boolean isEqualityTypePred(Expression predicate) {
		boolean returnValue = true;
		if (predicate instanceof MultiExpression) {
			
			MultiExpression multiExpr = (MultiExpression) predicate;
			MultiExpression mExpr;
			Expression exprTemp;
			for (int i=0; i < multiExpr.getExpressions().length;i++){
				exprTemp = multiExpr.getExpressions()[i];
				if (exprTemp instanceof MultiExpression) {					
					mExpr = (MultiExpression)exprTemp;
					returnValue = returnValue && isEqualityTypePred(mExpr);
				}	
				else {					
					returnValue = (multiExpr.getMultiType().compareTo(MultiType.EQUALS) == 0);
				}
			}
		} else {				
			if (predicate.toString().equals("TRUE")) {
				returnValue = false;
			}
			//Do something with non-multitype expression
		}
		return returnValue;
	}
}
