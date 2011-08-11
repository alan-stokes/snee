package uk.ac.manchester.cs.snee.operators.evaluator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEQueue;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * This {@link LosslessValveOperatorImpl} defines the first Valve Operator Model This
 * Valve operator Fills the queue When queue is filled up, increase the size of
 * the queue Supply data in Pull/Push mode
 * 
 * @author Praveen
 * 
 */
public class LosslessValveOperatorImpl extends ValveOperatorAbstractImpl {
	private Logger logger = Logger
			.getLogger(LosslessValveOperatorImpl.class.getName());
	// private ValveOperator valveOperator;
	private SNEEQueue<Output> inputBuffer;

	// private int maxBufferSize;

	public LosslessValveOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException, EvaluatorException {
		super(op, qid);
		inputBuffer = new SNEEQueue<Output>(maxBufferSize);
	}

	@Override
	protected boolean runProducer(Output output) {
		//System.out.println("Inside Producer: "+output);
		return inputBuffer.offer(output);
	}	

	@Override
	public Output getNext() {
		// TODO Auto-generated method stub
		return inputBuffer.poll();
	}

	@Override
	protected boolean isQueueEmpty() {
		return inputBuffer.isEmpty();
	}

	@Override
	public Output getNewestEntry() {
		// TODO Auto-generated method stub
		return inputBuffer.getNewest();
	}

	@Override
	public Output getOldestEntry() {
		// TODO Auto-generated method stub
		return inputBuffer.getOldest();
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return inputBuffer.size();
	}

	@Override
	public int getTotalObjectsInserted() {
		return inputBuffer.totalObjectsInserted();
	}

}
