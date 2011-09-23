package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.Observable;

import org.apache.log4j.Logger;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class MockPhysicalOperator extends EvaluatorPhysicalOperator {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2170163651568235694L;
  @SuppressWarnings("unused")
  private MockOperator mockOp;
  private static final Logger logger = Logger.getLogger(MockPhysicalOperator.class.getName());
	

	public MockPhysicalOperator(LogicalOperator op) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER: " + op);
		}

		// Instantiate deliver operator
		mockOp = (MockOperator) op;

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN");
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	//
	//	@Override
	//	public Collection<Output> getNext() throws ReceiveTimeoutException,
	//			SNEEException {
	//		// TODO Auto-generated method stub
	//		return null;
	//	}

	@Override
	public void open() {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(Observable obj, Object observed) {
		// TODO Auto-generated method stub

	}

}
