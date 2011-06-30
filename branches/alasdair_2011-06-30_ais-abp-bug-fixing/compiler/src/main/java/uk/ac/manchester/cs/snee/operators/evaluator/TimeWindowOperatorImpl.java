package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class TimeWindowOperatorImpl extends WindowOperatorImpl {

	Logger logger = Logger.getLogger(this.getClass().getName());

	private long lastWindowEvalTime;

	/**
	 * Stores the time unit used for expressing the slide
	 */
	private String slideWindowUnits;

	private CircularArray<TaggedTuple> buffer;

	private int tuplesSinceLastWindow = 0;

	private long lastTupleTick;

	private long nextWindowEvalTime;
	
	public TimeWindowOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException{
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER TimeWindowOperatorImpl() " + op);
		}
		// Set the size of the slide in milliseconds
		slide = windowOp.getTimeSlide() * 1000;
		slideWindowUnits = calculateWindowUnit();
		windowStart = windowStart * 1000;
		windowEnd = windowEnd * 1000;

		// Instantiate the buffer for storing tuples
		int maxBufferSize = SNEEProperties.getIntSetting(
				SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}
		buffer = new CircularArray<TaggedTuple>(maxBufferSize);
		
		if (logger.isTraceEnabled()) 
			logger.trace("\n\tStart (ms): " + windowStart + 
					"\n\tEnd (ms): " + windowEnd + "\n\tSlide (ms): " + slide );
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN TimeWindowOperatorImpl()");
		}
	}
	
	private String calculateWindowUnit() {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER calculateWindowUnit()");
		}
		//TODO: Use TimeUnit instead
		String slideUnit;
		if (slide % 86400000  == 0) {
			slideUnit = "DAYS";
		} else if (slide % 3600000 == 0) {
			slideUnit = "HOURS";
		} else if (slide % 60000 == 0) {
			slideUnit = "MINUTES";
		} else if (slide % 1000  == 0) {
			slideUnit = "SECONDS";
		} else {
			slideUnit = "MILLISECONDS";
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN calculateWindowUnit() slide unit " + 
					slideUnit);
		}
		return slideUnit;
	}

	public void open() 
	throws EvaluatorException {
		super.open();
		if (logger.isDebugEnabled())
			logger.debug("ENTER open()");
		/* aligns windows to granularity of slide definition */
		lastWindowEvalTime = 
			calculateFirstWindowEvalTime(System.currentTimeMillis());	
		nextWindowEvalTime = lastWindowEvalTime + slide;
		if (logger.isTraceEnabled())
			logger.trace("Last window eval time: " + 
				lastWindowEvalTime + 
				"\n\tNext window creation time: "+ nextWindowEvalTime);
		if (logger.isDebugEnabled())
			logger.debug("RETURN open()");
	}
	
	//	public Collection<Output> getNext() 
	//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("ENTER getNext()");
	//		}
	//		List<Output> returnWindows = new ArrayList<Output>();
	//
	//		// Emit an empty window for tick zero 
	//		if (lastWindowEvalTime == 0) {
	//			/* aligns windows to granularity of slide definition */
	//			lastWindowEvalTime = 
	//				calculateFirstWindowEvalTime(System.currentTimeMillis());
	//			Window window = new Window(new ArrayList<Tuple>());
	////			window.setEvalTime(lastWindowEvalTime);
	//			returnWindows.add(window);
	////			nextWindowIndex++;
	//			if (logger.isTraceEnabled())
	//				logger.trace("Emitting empty window as first window. Eval time: " + 
	//						lastWindowEvalTime);
	//		}
	//		
	//		do {
	//			long lastTupleReceivedEvalTime = receiveTuples();
	//			returnWindows.addAll(createWindows(lastTupleReceivedEvalTime)); 
	//		} while(returnWindows.isEmpty());
	//		
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("RETURN getNext() number of windows " + returnWindows.size());
	//		}
	//		return returnWindows;
	//	}
	
		/**
		 * Calculate the timestamp of the first window to be created based on the
		 * current timestamp and the units of the window slide.
		 *  
		 * @param currentTimeMillis
		 * @return
		 */
		private long calculateFirstWindowEvalTime(long currentTimeMillis) {
			if (logger.isTraceEnabled()) {
				logger.trace("ENTER calculateFirstWindowEvalTime() currentTS " + currentTimeMillis);
			}
			long windowTimestamp;
			if (slideWindowUnits.equalsIgnoreCase("MILLISECONDS")) {
				windowTimestamp = currentTimeMillis;
			} else if (slideWindowUnits.equalsIgnoreCase("SECONDS")) {
				windowTimestamp = 
					(long) Math.floor(currentTimeMillis/1000) * 1000;
			} else if (slideWindowUnits.equalsIgnoreCase("MINUTES")) {
				windowTimestamp = 
					(long) Math.floor(currentTimeMillis/60000) * 60000;
			} else if (slideWindowUnits.equalsIgnoreCase("HOURS")) {
				windowTimestamp = 
					(long) Math.floor(currentTimeMillis/3600000) * 3600000;
			} else {
				windowTimestamp = 
					(long) Math.floor(currentTimeMillis/86400000) * 86400000;
			}
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN calculateFirstWindowEvalTime() timestamp " + windowTimestamp);
			}
			return windowTimestamp;
		}

	public boolean isTimeScope() {
		return true;
	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() for query " + m_qid + " " +
					" with " + observed);
		List<Output> resultItems = new ArrayList<Output>();
		if (observed instanceof TaggedTuple){
			processTuple(observed, resultItems);
		} else if (observed instanceof List<?>) {
			List<Output> outputList = (List<Output>) observed;
			for (Output output : outputList) {
				if (output instanceof TaggedTuple) {
					processTuple(output, resultItems);
				}
			}
		} else {
			String msg = "Unknown or unexpected item type (" + 
			observed.getClass().getSimpleName() + ") in stream.";
			logger.warn(msg);
			//			throw new SNEEException(msg);
		}
		if (!resultItems.isEmpty()) {
			setChanged();
			notifyObservers(resultItems);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}

	private void processTuple(Object output, List<Output> resultItems) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processTuple() with " + output);
		TaggedTuple tuple = (TaggedTuple) output;
		tuplesSinceLastWindow++;
		buffer.add(tuple);
		lastTupleTick = tuple.getEvalTime();
		if (nextWindowEvalTime < lastTupleTick)
			resultItems.addAll(createWindows(lastTupleTick));
		if (logger.isTraceEnabled())
			logger.trace("RETURN processTuple() #tuples=" + buffer.size() +
					"\ntuplesSinceLastWindow=" + tuplesSinceLastWindow +
					"\nlastTupleTick=" + lastTupleTick);
	}

//	public Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER getNext()");
//		}
//		List<Output> returnWindows = new ArrayList<Output>();
//
//		// Emit an empty window for tick zero 
//		if (lastWindowEvalTime == 0) {
//			/* aligns windows to granularity of slide definition */
//			lastWindowEvalTime = 
//				calculateFirstWindowEvalTime(System.currentTimeMillis());
//			Window window = new Window(new ArrayList<Tuple>());
////			window.setEvalTime(lastWindowEvalTime);
//			returnWindows.add(window);
////			nextWindowIndex++;
//			if (logger.isTraceEnabled())
//				logger.trace("Emitting empty window as first window. Eval time: " + 
//						lastWindowEvalTime);
//		}
//		
//		do {
//			long lastTupleReceivedEvalTime = receiveTuples();
//			returnWindows.addAll(createWindows(lastTupleReceivedEvalTime)); 
//		} while(returnWindows.isEmpty());
//		
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN getNext() number of windows " + returnWindows.size());
//		}
//		return returnWindows;
//	}

	/**
	 * Create windows based on the start, end, and slide parameters.
	 * 
	 * @param lastTupleReceivedEvalTime
	 * @return
	 */
	private List<Output> createWindows(long lastTupleReceivedEvalTime) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createWindows() last tuple received time " + lastTupleReceivedEvalTime);
		}
		List<Output> returnWindows = new ArrayList<Output>();
//		/* Calculate evaluation time of next window based on slide */
//		nextWindowEvalTime = lastWindowEvalTime + slide;
//		if (logger.isTraceEnabled()) {
//			logger.trace("Next window creation time: "+ nextWindowEvalTime);
//		}
		while (nextWindowEvalTime < lastTupleReceivedEvalTime) {
				
			/* Find the index of the oldest tuple in the window */
			int nextTupleIndex = findTupleIndex(nextWindowEvalTime + windowStart);

			List<Tuple> tupleList = populateWindowTuples(nextTupleIndex, nextWindowEvalTime);

			Window window = new Window(tupleList);		
			returnWindows.add(window);
			/* Update control variables */
//			nextWindowIndex++;
			lastWindowEvalTime = nextWindowEvalTime;
			nextWindowEvalTime = lastTupleReceivedEvalTime + slide;
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createWindows() number of windows created " + returnWindows.size());
		}
		return returnWindows;
	}

	/**
	 * Identify which tuples should be included in the window
	 * 
	 * @param nextTupleIndex
	 * @param nextWindowEvalTime
	 * @return
	 */
	private List<Tuple> populateWindowTuples(int nextTupleIndex,
			long nextWindowEvalTime) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER populateWindowTuples() next tuple index " + nextTupleIndex + 
					" next window eval time " + nextWindowEvalTime);
		}
		List<Tuple> tupleList = new ArrayList<Tuple>();
		while (nextTupleIndex < buffer.size()) {
			TaggedTuple taggedTuple = (TaggedTuple) buffer.get(nextTupleIndex);
			if (taggedTuple.getEvalTime() <= nextWindowEvalTime) {
				tupleList.add(taggedTuple.getTuple());
				nextTupleIndex++;
			} else {
				/* Seen a tuple that is newer than the window evaluation time */
				if (logger.isTraceEnabled()) 
					logger.trace("Tuple has newer timestamp (" + taggedTuple.getEvalTime() +
							") than window evaluation time (" + nextWindowEvalTime + ").");
				break;
			}
		}
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN populateWindowTuples() window size " + tupleList.size());
		return tupleList;
	}

	/**
	 * Find the index of the tuple in the buffer with a timestamp
	 * greater or equal to a given timestamp
	 * 
	 * @param tupleTime timestamp to find the corresponding tuple index for
	 * @return buffer index for corresponding tuple
	 */
	private int findTupleIndex(long tupleTime) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER findTupleIndex() tuple time to find " + tupleTime);
		}
		int index = 0;
		for (Object ob : buffer) {
			TaggedTuple tuple = (TaggedTuple) ob;
			if (tuple.getEvalTime() >= tupleTime) {
				break;
			} else {
				index++;
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN findTupleIndex() tuple index " + index);
		}
		return index;
	}
	
}