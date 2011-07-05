package uk.ac.manchester.cs.snee.operators.evaluator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * This {@link LossyValveOperatorImpl} defines the second Valve Operator Model.
 * This Valve operator fills the queue as data comes in. When queue is filled
 * up, drop the tuples using CircularArray. Supply data in Pull/Push mode
 * 
 * @author Praveen
 * 
 */
public class LossyValveOperatorImpl extends ValveOperatorAbstractImpl {

	Logger logger = Logger.getLogger(LossyValveOperatorImpl.class.getName());

	private CircularArray<Output> inputBufferQueue;

	public LossyValveOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("Enter LossyValveOperatorImpl with query "+qid);
		}
		inputBufferQueue = new CircularArray<Output>(maxBufferSize);
		
		if (logger.isDebugEnabled()) {
			logger.debug("Exit LossyValveOperatorImpl with query "+qid);
		}
	}

	@Override
	protected boolean runProducer(Output output) {
		if (logger.isDebugEnabled()) {
			logger.debug("Adding data "+output);
		}
		return inputBufferQueue.add(output);
	}

	@Override
	protected boolean isQueueEmpty() {
		return inputBufferQueue.isEmpty();
	}

	@Override
	public Output getNext() {
		return inputBufferQueue.poll();
	}

}
