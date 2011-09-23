package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class TupleWindowOperatorImpl extends WindowOperatorImpl {
	
	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 1191917460879844306L;
  private static final Logger logger = Logger.getLogger(TupleWindowOperatorImpl.class.getName());
  private int windowSize;
	private Tuple[] buffer;
	private int nextIndex = 0;

	public TupleWindowOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER TupleWindowOperatorImpl() with " + op);
		}
		//FIXME: Slide type is being set incorrectly, needing to use getTimeSlide()
		// Set the size of the slide 
//		slide = windowOp.getRowSlide();
		slide = windowOp.getTimeSlide();
		windowSize = windowEnd - windowStart;

		// Instantiate the buffer for storing tuples
		buffer = new Tuple[windowSize];
		
		if (logger.isTraceEnabled()) 
			logger.trace("Window size: " + windowSize + " Slide: " + slide);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN TupleWindowOperatorImpl()");
		}
	}
	
	@Override
	public boolean isTimeScope() {
		return false;
	}

	@SuppressWarnings("unchecked")
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

	private void processTuple(Object observed, List<Output> resultItems) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processTuple() with " + observed);
		Tuple tuple = ((TaggedTuple) observed).getTuple();
		if (nextIndex >= 0) {
			buffer[nextIndex] = tuple;
		}
		nextIndex++;
		logger.trace("nextIndex=" + nextIndex);
		if (nextIndex == windowSize) {
			resultItems.add(generateWindow());
			resetBuffer();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processTuple()");
	}

	private void resetBuffer() {
		if (logger.isTraceEnabled())
			logger.trace("ENTER resetBuffer() windowSize=" + windowSize + 
					" slide=" + slide);
		nextIndex = windowSize - slide;
		if (nextIndex > 0) {
			int shiftFactor = windowSize - nextIndex;
			for (int i = 0; i < nextIndex; i++) {
				buffer[i] = buffer[i + shiftFactor];
			}
		} 
		if (logger.isTraceEnabled())
			logger.trace("RETURN resetBuffer(), nextIndex=" + nextIndex +
					"\n" + buffer);
	}

	private Window generateWindow() {
		if (logger.isTraceEnabled())
			logger.trace("ENTER generateWindow()");
		List<Tuple> tupleList = new ArrayList<Tuple>();
		for (int i = 0; i < buffer.length ; i++) {
			tupleList.add(buffer[i]);
		}
		Window window = new Window(tupleList);
		if (logger.isTraceEnabled())
			logger.trace("RETURN generateWindow() with " + window);
		return window;
	}

	//	public Collection<Output> getNext() 
	//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("ENTER getNext()");
	//		}
	//
	//		// Create collection for operator results
	//		List<Output> resultItems = new ArrayList<Output>();
	//
	//		while(resultItems.isEmpty()){
	//			Collection<Output> bagOfTuples = child.getNext();
	//			for (Output item : bagOfTuples) {
	//				if (item instanceof TaggedTuple){
	//					Tuple tuple = ((TaggedTuple) item).getTuple();
	//					buffer.add(tuple);
	//					tuplesSinceLastWindow++;
	//					if (logger.isTraceEnabled()) {
	//						logger.trace("Number of received tuples: " + 
	//								buffer.size() +
	//								", Tuples since last window: " + 
	//								tuplesSinceLastWindow);
	//					}
	//				} else {
	//					String msg = "Unknown or unexpected item type (" + 
	//						item.getClass().getSimpleName() + ") in stream.";
	//					logger.warn(msg);
	//					throw new SNEEException(msg);
	//				}
	//				if (tuplesSinceLastWindow >= slide) {
	//					if (logger.isTraceEnabled())
	//						logger.trace("tuples seen >= slide. Buffer size: " + 
	//								buffer.size());
	//					List<Tuple> tupleList = new ArrayList<Tuple>();
	//					int newestTupleIndex = buffer.size();
	//					for (int i = windowSize; i > 0 ; i--) {
	//						int tupleOffset = newestTupleIndex - i;
	//						if (logger.isTraceEnabled()){ 
	//							logger.trace("i:" + i + " Tuple index: " + newestTupleIndex + 
	//									" Tuple offset: " + tupleOffset);
	//						}
	//						if (tupleOffset >= 0) {
	//							Tuple tuple = (Tuple) buffer.get(tupleOffset);
	//							tupleList.add(tuple);
	//						}
	//					}
	//					resultItems.add(new Window(tupleList));
	//					tuplesSinceLastWindow = 0;
	////					nextWindowIndex++;
	//				}
	//			}
	//		}
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("RETURN getNext() number of windows " + 
	//					resultItems.size());
	//		}
	//		return resultItems;
	//	}

}
