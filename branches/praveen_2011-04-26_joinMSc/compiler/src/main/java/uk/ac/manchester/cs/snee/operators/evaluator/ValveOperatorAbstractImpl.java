package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;
import uk.ac.manchester.cs.snee.types.Duration;

public abstract class ValveOperatorAbstractImpl extends EvaluationOperator {

	private Logger logger = Logger
			.getLogger(LosslessValveOperatorImpl.class.getName());
	protected ValveOperator valveOperator;
	protected int maxBufferSize;
	private Duration queueScanInterval;
	private Timer timer;
	private int objectSizeToPush;

	/**
	 * The constructor method for the abstract class. All the variables that has
	 * to be initialised in common for all the valve implementations will be
	 * done in this constructor
	 * 
	 * @param op
	 * @param qid
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 */
	public ValveOperatorAbstractImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException {
		super(op, qid);

		// Instantiate valve operator
		valveOperator = (ValveOperator) op;
		maxBufferSize = SNEEProperties
				.getIntSetting(SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);

		queueScanInterval = valveOperator.getQueueScanInterval();
		objectSizeToPush = valveOperator.getOutputSize();

	}

	@Override
	public void open() throws EvaluatorException {
		super.open();
		if (valveOperator.isPushBasedOperator()) {
			//System.out.println("Inside Open");
			timer = new Timer();
			ConsumerTask consumerTask = new ConsumerTask();
			timer.schedule(consumerTask, 0, queueScanInterval.getDuration());
		}
	}

	@Override
	public void update(Observable obj, Object observed) {
		//System.out.println("observed: "+observed);
		if (observed instanceof List<?>) {
			
			List<Output> outputList = (List<Output>) observed;
			for (Output output : outputList) {
				//System.out.println("here"+output);
				if (!runProducer(output)) {
					
					if (logger.isDebugEnabled()) {
						logger.debug("Object dropped: " + output);
					}
				}
			}
		} else if (observed instanceof Output) {
			//System.out.println("Second");
			if (!runProducer((Output) observed)) {
				if (logger.isDebugEnabled()) {
					logger.debug("Object dropped: " + observed);
				}
			}

		}

	}

	/**
	 * Each Valve implementation is supposed to have its own version of queue
	 * and is maintained in a distinct way. The method runProducer aims to
	 * insert into the queue the incoming data items, in the manner dictated by
	 * the queue management of that particular valve implementation.
	 * 
	 * @param output
	 * @return
	 */
	protected abstract boolean runProducer(Output output);	

	/**
	 * Abstract method to check if the queue is empty
	 * 
	 * @return
	 */
	protected abstract boolean isQueueEmpty();

	/**
	 * This method returns the head of the queue, when asked for by the parent
	 * operator. This method thus enables the working of the Valve operator in a
	 * pull mode, which supplies data on demand.
	 * 
	 * @return Output
	 */
	public abstract Output getNext();

	/**
	 * This class runs the Timer for the push based valve operator
	 * 
	 * @author Praveen
	 * 
	 */
	private class ConsumerTask extends TimerTask {

		@Override
		public void run() {
			//System.out.println("Running");
			//runConsumerForPushyValve();
			//System.out.println("*************************************************");
			//System.out.println("Is Queue Empty: "+isQueueEmpty());
			if (!isQueueEmpty()) {
				List<Output> resultList = new ArrayList<Output>();
				for (int i = 0; i < objectSizeToPush; i++) {
					Output object = getNext();
					//System.out.println("Pushing object: "+object);
					if (object != null) {
						//System.out.println("Pushing object: "+object);
						resultList.add(object);
					}
				}
				if (!resultList.isEmpty()) {
					setChanged();
					notifyObservers(resultList);
				}
			}
		}

	}

}
