package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
	private int numTuplesToDrop = 1;
	private String opId = "";

	public LossyValveOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("Enter LossyValveOperatorImpl with query " + qid);
		}
		opId = op.getID();
		inputBufferQueue = new CircularArray<Output>(maxBufferSize, opId);
		this.tupleDropPolicy = valveOperator.getTupleDropPolicy();
		if (valveOperator.getLoadShedRate() == 1.0) {
			this.numTuplesToDrop = 1;
		} else {
			this.numTuplesToDrop = (int) ((valveOperator.getLoadShedRate()/100) * maxBufferSize);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("The tuple drop policy is : "+tupleDropPolicy+ " and the Number of tuples to drop is:: " + numTuplesToDrop);
		}
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
			handleFIFODrop();
			break;
		case LIFO:
			inputBufferQueue.dropLastInsertedObject();
			break;
		case SAMPLE:
			handleSampleDrop();
			/*
			 * if (!canDropObject()) { if (logger.isInfoEnabled()) {
			 * logger.info("Object Dropped due to sampling"); } return false; }
			 */
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
						+ inputBufferQueue.size() + " " + this);
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Method to drop tuples in random sample of fixed size
	 */
	private void handleSampleDrop() {
		if (inputBufferQueue.size() == (inputBufferQueue.capacity())) {
			Set<Integer> sampleDropArray = getRandomIndexes();
			List<Output> newList = new ArrayList<Output>();
			for (int i = 0; i < maxBufferSize; i++) {
				if (sampleDropArray.contains(new Integer(i))) {
					// Dropping the random sample
					if (logger.isInfoEnabled()) {
						logger.info("Object Dropped due to sampling in Valve with id: "+opId);
					}
					inputBufferQueue.poll();
					continue;
				}
				newList.add(inputBufferQueue.poll());
			}
			inputBufferQueue = new CircularArray<Output>(maxBufferSize, opId, newList);
		}
	}

	/**
	 * method to get a unique set of random numbers of the size of
	 * numTuplesToDrop size
	 * 
	 * @return
	 */
	private Set<Integer> getRandomIndexes() {
		Set<Integer> uniqueIndexes = new HashSet<Integer>(numTuplesToDrop);
		Random random = new Random();
		do {
			uniqueIndexes.add(random.nextInt(maxBufferSize));
		} while (uniqueIndexes.size() != numTuplesToDrop);

		return uniqueIndexes;
	}

	/**
	 * method to handle the object drop in FIFO mode
	 * 
	 */
	private void handleFIFODrop() {
		if (inputBufferQueue.size() == (inputBufferQueue.capacity())) {
			if (logger.isDebugEnabled()) {
				logger.debug("The input buffer size is: "+ inputBufferQueue.size()
						+" and the capacity of the buffer is: "+inputBufferQueue.capacity()+
						" and both are equal, so going to drop");
			}
			for (int i = 0; i < numTuplesToDrop; i++) {
				if (logger.isInfoEnabled()) {
					logger.info("Object Dropped by FIFO");
				}
				inputBufferQueue.poll();
			}
		}

	}

	// private boolean canDropObject() {
	// boolean canAdd = true;
	// int randomNumber = (int) (Math.random() * 100);
	// if (randomNumber <= samplingRate) {
	// canAdd = false;
	// }
	// return canAdd;
	// }

	@Override
	protected boolean isQueueEmpty() {
		return inputBufferQueue.isEmpty();
	}

	@Override
	public Output getNext() {
		if (logger.isDebugEnabled()) {
			logger.debug("This is before Polling:***" + valveOperator.getID()
					+ inputBufferQueue.size() + this);
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
					+ inputBufferQueue.getOperatorId() + " " + this);
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
