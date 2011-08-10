package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.ExchangeOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.types.Duration;

public class ExchangeOperatorImpl extends EvaluationOperator {

	private Logger logger = Logger.getLogger(ExchangeOperatorImpl.class
			.getName());
	private ExchangeOperator exchangeOperator;
	private CircularArray<Output> buffer;
	private int maxBufferSize = 0;
	private Timer timer;
	private int objectSizeToPush;
	private Duration queueScanInterval;
	

	public ExchangeOperatorImpl(LogicalOperator op, int qid)
			throws SNEEException, SchemaMetadataException,
			SNEEConfigurationException, EvaluatorException {
		super(op, qid);

		// Instantiate exchange operator
		exchangeOperator = (ExchangeOperator) op;
		maxBufferSize = SNEEProperties
				.getIntSetting(SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		buffer = new CircularArray<Output>(maxBufferSize);
		objectSizeToPush = exchangeOperator.getOutputSize();
		queueScanInterval = exchangeOperator.getQueueScanInterval();
	}

	@Override
	public void open() throws EvaluatorException {
		super.open();

		timer = new Timer();
		ConsumerTask consumerTask = new ConsumerTask();
		timer.schedule(consumerTask, 0, queueScanInterval.getDuration());

	}

	@Override
	public void update(Observable obj, Object observed) {

		System.out.println("Test");
		//List<Output> resultList = new ArrayList<Output>();
		if (observed instanceof List<?>) {
			List<Output> outputList = (List<Output>) observed;
			for (Output output : outputList) {
				if (!producerTask(output)) {
					if (logger.isTraceEnabled()) {
						logger.trace("Object dropped: " + output);
					}
				}
				// resultList.add(output);
			}
		} else if (observed instanceof Output) {
			if (!producerTask((Output) observed)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Object dropped: " + (Output) observed);
				}
			}
			// resultList.add((Output) observed);
		}
		/*if (!resultList.isEmpty()) {
			setChanged();
			notifyObservers(resultList);
		}*/

	}

	public boolean producerTask(Output output) {
		return buffer.add(output);
	}

	/**
	 * This class runs the Timer for the Exchange operator
	 * to generate output for the next operator in the query plan
	 * 
	 * @author Praveen
	 * 
	 */
	private class ConsumerTask extends TimerTask {

		@Override
		public void run() {
			if (!buffer.isEmpty()) {
				List<Output> resultList = new ArrayList<Output>();
				for (int i = 0; i < objectSizeToPush; i++) {
					Output object = buffer.poll();
					// System.out.println("Pushing object: "+object);
					if (object != null) {
						// System.out.println("Pushing object: "+object);
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
