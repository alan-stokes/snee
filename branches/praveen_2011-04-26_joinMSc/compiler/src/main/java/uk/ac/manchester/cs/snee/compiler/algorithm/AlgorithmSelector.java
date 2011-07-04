/**
 * 
 */
package uk.ac.manchester.cs.snee.compiler.algorithm;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;

/**
 * @author Praveen
 * 
 */
public class AlgorithmSelector {
	Logger logger = Logger.getLogger(this.getClass().getName());

	private double thresholdRate;

	public AlgorithmSelector() throws SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AlgorithmSelector()");
		}
		thresholdRate = SNEEProperties
				.getIntSetting(SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN AlgorithmSelector()");
		}
	}

	/**
	 * This method initiates the selection of algorithms for the different
	 * operators present in the dlaf. This renames the DLAF to DLAF'.
	 * 
	 * @param dlaf
	 * @param qos
	 * @return
	 */
	public DLAF doAlgorithmSelection(DLAF dlaf, QoSExpectations qos) {
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
		replaceWithAlgorithms(dlaf, qos);
		return dlaf;

	}

	/**
	 * This method iterates the dlaf and assigns the specific operator with its
	 * associated algorithms, by routing the calls to appropriate methods This
	 * algorithm selection is performed only for out of SN operators, and in
	 * network algorithm selection is a separate process
	 * 
	 * @param dlaf
	 * @param qos
	 */
	private void replaceWithAlgorithms(DLAF dlaf, QoSExpectations qos) {
		Iterator<LogicalOperator> opIter = dlaf
				.operatorIterator(TraversalOrder.PRE_ORDER);
		LogicalOperator op;
		while (opIter.hasNext()) {
			op = opIter.next();
			if (SourceType.SENSOR_NETWORK != op.getOperatorSourceType()) {
				if (op instanceof JoinOperator) {
					setJoinOperatorAlgorithm((JoinOperator) op);
				} else if (op instanceof ValveOperator) {
					setValveOperatorAlgorithm((ValveOperator) op, qos);
				}
			}
		}

	}

	/**
	 * Set the algorithm with which the valve operator would work
	 * 
	 * @param valveOperator
	 * @param qos
	 */
	private void setValveOperatorAlgorithm(ValveOperator valveOperator,
			QoSExpectations qos) {
		// TODO set it via property

		if (valveOperator.getInput(0).getSourceRate() > thresholdRate) {
			valveOperator.setPushBasedOperator(false);

			// This following code sets the parent operator which is a
			// join operator to work in pull mode
			valveOperator.getParent().setGetDataByPullModeOperator(true);
		} else {
			valveOperator.setPushBasedOperator(true);
			valveOperator.getParent().setGetDataByPullModeOperator(false);
		}
		// Lossy or Lossless is to be determined via QoS parameters
		if (qos.isTupleLossAllowed()) {
			valveOperator.setAlgorithm(ValveOperator.TUPLE_DROP_MODE);
		} else {
			valveOperator.setAlgorithm(ValveOperator.GROW_SIZE_MODE);
		}
	}

	/**
	 * Set the algorithm with which the join operator will work
	 * 
	 * @param joinOperator
	 */
	private void setJoinOperatorAlgorithm(JoinOperator joinOperator) {
		if (joinOperator.getPredicate() instanceof NoPredicate
				|| !(joinOperator.getInput(0) instanceof ValveOperator)) {
			// joinOperatorImpl = new JoinOperatorImpl(op, qid);
			joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
		} else {
			Expression predicate = joinOperator.getPredicate();
			// check if all the expressions in the predicate are of
			// equality type and only in such cases perform HashJoin
			if (isEqualityTypePred(predicate)) {
				// joinOperatorImpl = new HashJoinOperatorImpl(op, qid);
				joinOperator.setAlgorithm(JoinOperator.SHJ_MODE);
			} else {
				// joinOperatorImpl = new JoinOperatorImpl(op, qid);
				joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
			}
		}

	}

	/**
	 * Method that checks if all the join condition within the predicate are of
	 * equality type
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
			for (int i = 0; i < multiExpr.getExpressions().length; i++) {
				exprTemp = multiExpr.getExpressions()[i];
				if (exprTemp instanceof MultiExpression) {
					mExpr = (MultiExpression) exprTemp;
					returnValue = returnValue && isEqualityTypePred(mExpr);
				} else {
					returnValue = (multiExpr.getMultiType().compareTo(
							MultiType.EQUALS) == 0);
				}
			}
		} else {
			if (predicate.toString().equals("TRUE")) {
				returnValue = false;
			}
			// Do something with non-multitype expression
		}
		return returnValue;
	}
}
