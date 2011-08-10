package uk.ac.manchester.cs.snee.operators.factory;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluationOperator;
import uk.ac.manchester.cs.snee.operators.evaluator.HashJoinOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.JoinOperatorImpl;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public abstract class JoinOperatorImplAbstractFactory {

	public static EvaluationOperator getJoinOperatorImpl(LogicalOperator op,
			int qid) throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException, EvaluatorException {
		EvaluationOperator joinOperatorImpl;
		if (op.getPredicate() instanceof NoPredicate) {
			joinOperatorImpl = new JoinOperatorImpl(op, qid);
		} else {			
			Expression predicate = op.getPredicate();			
			//check if all the expressions in the predicate are of 
			//equality type and only in such cases perform HashJoin
			if (isEqualityTypePred(predicate)) {
				joinOperatorImpl = new HashJoinOperatorImpl(op, qid);
			} else {
				joinOperatorImpl = new JoinOperatorImpl(op, qid);
			}
		}
		return joinOperatorImpl;
	}

	private static boolean isEqualityTypePred(Expression predicate) {
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
