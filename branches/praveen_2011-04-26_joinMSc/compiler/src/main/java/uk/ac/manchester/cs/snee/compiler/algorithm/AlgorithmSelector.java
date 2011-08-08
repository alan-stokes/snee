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
import uk.ac.manchester.cs.snee.operators.logical.OperatorDataType;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;

/**
 * This class is intended for purely assigning algorithm to operators that fall
 * out of the sensor network. The in-sensor-network algorithm selection is a
 * separate process and happens further down the SNEE stack
 * 
 * @author Praveen
 * 
 */
public class AlgorithmSelector {
	Logger logger = Logger.getLogger(this.getClass().getName());

	private double thresholdRate;
	private boolean isJoinNLJOnly = false;

	public AlgorithmSelector() throws SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AlgorithmSelector()");
		}
		isJoinNLJOnly = SNEEProperties
				.getBoolSetting(SNEEPropertyNames.COMPILER_ALGORTHM_SELECTION_NLJ_ONLY);
		thresholdRate = SNEEProperties
				.getDoubleSetting(SNEEPropertyNames.COMPILER_ALGORTHM_SELECTION_THRESHOLD_RATE);

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
	 * network algorithm selection is a separate process.
	 * 
	 * The join algorithm selection should be performed before valve operator
	 * algorithm selection
	 * 
	 * @param dlaf
	 * @param qos
	 */
	private void replaceWithAlgorithms(DLAF dlaf, QoSExpectations qos) {
		Iterator<LogicalOperator> opIter = dlaf
				.operatorIterator(TraversalOrder.POST_ORDER);
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
	 * Valve operators will be placed to buffer the join operations from getting
	 * overloaded. So valve operators would be placed between each join operator
	 * and its children in an out-SN setting. The join operators would then have
	 * to be changed to work in a pull mode or the conventional iterator model
	 * rather than the current subscribe mode. The data from the join operator
	 * would then be pubished as it is being done now and the rest of the
	 * operators would operate in the same Publish-Subscribe model.
	 * 
	 * Implementation
	 * 
	 * 1. If the parent join operator has the algorithm selected as Hash join,
	 * the valve operator will subscribe to the data from its child, and hold on
	 * to it until pulled by the join operator above. So the parent join
	 * operator is informed to work in this mode.
	 * 
	 * 2. If the source rate of the child operator for the valve is above a
	 * particular threshold value as defined in the SNEE.properties file, enable
	 * the valve operator to subscribe to the data from its child, and hold on
	 * to it until pulled by the join operator above. So the parent join
	 * operator is informed to work in this mode.
	 * 
	 * 3. If the source rate of the child operator for the valve is below a
	 * particular threshold value as defined in the SNEE.properties file, the
	 * valve operator works in the traditional publish mode and the join
	 * operator above will also work in the publish-subscribe mode, i.e. no
	 * change from the current mode
	 * 
	 * @param valveOperator
	 * @param qos
	 */
	private void setValveOperatorAlgorithm(ValveOperator valveOperator,
			QoSExpectations qos) {

		// If atleast one of the valve operators in a join is having a
		// source rate greater than the threshold rate set the join
		// operator as Pull based and make both valve operators
		// of the join to work in Pull mode
		/*JoinOperator joinOp = (JoinOperator) valveOperator.getParent();
		if (valveOperator.getInput(0).getSourceRate() > thresholdRate
				|| (JoinOperator.LHJ_MODE.equals(joinOp.getAlgorithm()))
				|| (JoinOperator.RHJ_MODE.equals(joinOp.getAlgorithm()))) {
			if (valveOperator.isPushBasedOperator()) {
				// Setting both valve operator to work in Pull mode in case
				// at least one of the valve operator has to deal with stream
				// of arrival rate greater than the threshold rate
				// OR if the join algorithm is of Hash Join
				((ValveOperator) joinOp.getInput(0))
						.setPushBasedOperator(false);
				((ValveOperator) joinOp.getInput(1))
						.setPushBasedOperator(false);
				// valveOperator.setPushBasedOperator(false);

				// This following code sets the parent operator which is a
				// join operator to work in pull mode
				joinOp.setGetDataByPullModeOperator(true);
			}
		} else {
			valveOperator.setPushBasedOperator(true);
			joinOp.setGetDataByPullModeOperator(false);
		}*/
		//Till now there is no use case for making the valve operator
		//a push based operator. When such a use case comes, the logic
		//to set the push based functionality of  the valve operator
		//should go here.
		valveOperator.setPushBasedOperator(false);
//		if (valveOperator.getParent() instanceof JoinOperator) {
//			JoinOperator joinOp = (JoinOperator) valveOperator.getParent();
//			valveOperator.setPushBasedOperator(false);
//			joinOp.setGetDataByPullModeOperator(true);
//
//		} else {
//			valveOperator.setPushBasedOperator(true);
//		}
		// Lossy or Lossless is to be determined via QoS parameters
		if (qos.isTupleLossAllowed()) {
			valveOperator.setAlgorithm(ValveOperator.TUPLE_DROP_MODE);
			valveOperator.setSamplingRate(qos.getSamplingRate());
			valveOperator.setTupleDropPolicy(qos.getTupleDropPolicy());
		} else {
			// TODO this has to replaced with TUPLE_OFFLOAD_MODE, once that
			// implementation is done
			valveOperator.setAlgorithm(ValveOperator.GROW_SIZE_MODE);
		}
	}

	/**
	 * Set the algorithm with which the join operator will work
	 * 
	 * @param joinOperator
	 */
	private void setJoinOperatorAlgorithm(JoinOperator joinOperator) {
		if (isJoinNLJOnly || joinOperator.getPredicate() instanceof NoPredicate
				|| !(joinOperator.getInput(0) instanceof ValveOperator)) {
			// joinOperatorImpl = new JoinOperatorImpl(op, qid);
			joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
		} else {
			Expression predicate = joinOperator.getPredicate();
			// check if all the expressions in the predicate are of
			// equality type and only in such cases perform HashJoin
			if (isEqualityTypePred(predicate)) {
				// joinOperatorImpl = new HashJoinOperatorImpl(op, qid);
				setHashJoinAlgorithm(joinOperator);// joinOperator.setAlgorithm(JoinOperator.SHJ_MODE);
			} else {
				// joinOperatorImpl = new JoinOperatorImpl(op, qid);
				joinOperator.setAlgorithm(JoinOperator.NLJ_MODE);
			}
		}
		//The join operator has a state management operator to manage its
		//state, so just pull data from the state management operator
		if (!joinOperator.getInput(0).isPushBasedOperator()) {
			joinOperator.setGetDataByPullModeOperator(true);
		}

	}

	/**
	 * 
	 * If there is a join between stream & relation - relation is hashed window
	 * & relation - relation is hashed window & window - one of the windows
	 * which has the lowest source rate is hashed
	 * 
	 * @param joinOperator
	 */
	private void setHashJoinAlgorithm(JoinOperator joinOperator) {
		LogicalOperator leftOperator = joinOperator.getInput(0);
		LogicalOperator rightOperator = joinOperator.getInput(1);
		OperatorDataType leftOperatorDataType = leftOperator
				.getOperatorDataType();
		OperatorDataType rightOperatorDataType = rightOperator
				.getOperatorDataType();

		if (leftOperatorDataType == OperatorDataType.STREAM
				&& rightOperatorDataType == OperatorDataType.RELATION) {
			joinOperator.setAlgorithm(JoinOperator.RHJ_MODE);
		} else if (leftOperatorDataType == OperatorDataType.WINDOWS
				&& rightOperatorDataType == OperatorDataType.RELATION) {
			joinOperator.setAlgorithm(JoinOperator.RHJ_MODE);
		} else if (leftOperatorDataType == OperatorDataType.WINDOWS
				&& rightOperatorDataType == OperatorDataType.WINDOWS) {
			if (leftOperator.getSourceRate() > rightOperator.getSourceRate()) {
				joinOperator.setAlgorithm(JoinOperator.RHJ_MODE);
			} else {
				joinOperator.setAlgorithm(JoinOperator.LHJ_MODE);
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("Should never be here");
			}
			joinOperator.setAlgorithm(JoinOperator.LHJ_MODE);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("The Algorithm Chosen is"
					+ joinOperator.getAlgorithm());
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
