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
		inputBufferQueue = new CircularArray<Output>(maxBufferSize, op.getID());
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
				if (logger.isInfoEnabled()) {
					logger.info("Object Dropped due to sampling");
				}
				return false;
			}
			break;
		default:
			break;
		}

		if (inputBufferQueue.add(output)) {
			if (logger.isInfoEnabled()) {
				logger.info("Valve Operator with id: " + valveOperator.getID()
						+ " using CQ with id: "
						+ inputBufferQueue.getOperatorId()
						+ " has a total number of "
						+ inputBufferQueue.totalObjectsInserted() + " inserted"
						+ "and has a current siize of"
						+ inputBufferQueue.size()+" "+this);
			}
			return true;
		} else {
			return false;
		}
	}

	private boolean canDropObject() {
		boolean canAdd = true;
		int randomNumber = (int) (Math.random() * 100);
		if (randomNumber <= samplingRate) {
			canAdd = false;
		}
		return canAdd;
	}

	@Override
	protected boolean isQueueEmpty() {
		return inputBufferQueue.isEmpty();
	}

	@Override
	public Output getNext() {
		if (logger.isDebugEnabled()) {
			logger.debug("This is before Polling:***" + valveOperator.getID()
					+ inputBufferQueue.size()+ this);
		}
		return inputBufferQueue.poll();
	}

	@Override
	public Output getNewestEntry() {
		Output output = null;

		output = inputBufferQueue.getLatest();
		if (logger.isDebugEnabled()) {
			logger.debug("This is the output obtained:*** from valve operator with id: "
					+ valveOperator.getID()
					+ output
					+ "havng a size of: "
					+ inputBufferQueue.size()
					+ " using CQ of id: "
					+ inputBufferQueue.getOperatorId()+" "+this);
		}
		return output;
	}

	@Override
	public Output getOldestEntry() {
		return inputBufferQueue.getOldest();
	}

	@Override
	public int getSize() {
		return inputBufferQueue.size();
	}

}
