package uk.ac.manchester.cs.snee.operators.evaluator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.TupleDropPolicy;
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
	private TupleDropPolicy tupleDropPolicy;
	private int samplingRate = 0;

	public LossyValveOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("Enter LossyValveOperatorImpl with query " + qid);
		}
		inputBufferQueue = new CircularArray<Output>(maxBufferSize);
		this.tupleDropPolicy = valveOperator.getTupleDropPolicy();
		this.samplingRate = valveOperator.getSamplingRate();

		if (logger.isDebugEnabled()) {
			logger.debug("Exit LossyValveOperatorImpl with query " + qid);
		}
	}

	@Override
	protected boolean runProducer(Output output) {
		if (logger.isDebugEnabled()) {
			logger.debug("Adding data " + output);
		}
		switch (tupleDropPolicy) {
		case FIFO:
			break;
		case LIFO:
			inputBufferQueue.dropLastInsertedObject();
			break;
		case SAMPLE:
			if (!canDropObject()) {
				return false;
			}
			break;
		default:
			break;
		}
		return inputBufferQueue.add(output);
	}

	private boolean canDropObject() {
		boolean canAdd = true;
		int randomNumber = (int) (Math.random() * 100);
		if (randomNumber <= samplingRate) {
			canAdd = false;
		}
		if (canAdd == true) {
			System.out.println("can add");
		}
		return canAdd;
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
