package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class TupleWindowOperatorImpl extends WindowOperatorImpl {
	
	private int windowSize;
	private Tuple[] buffer;
	private int nextIndex = 0;
	private List<Tuple> prevSlideTuples;
	private boolean isFirst;

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
		prevSlideTuples = new ArrayList<Tuple>(1);
		isFirst = true;
		
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
	
	@Override
	public void generateAndUpdate(List<Output> resultItems) {
		if (sourceOperator.getSize() + prevSlideTuples.size() >= windowSize) {
			resultItems.add(generateWindow());
		}
		
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
		List<Tuple> tupleList = getTupleList();		
		Window window = new Window(tupleList);
		if (logger.isTraceEnabled())
			logger.trace("RETURN generateWindow() with " + window);
		return window;
	}

	/*private List<Tuple> getTupleList1() {
		List<Tuple> tupleList = new ArrayList<Tuple>();
		if  (!windowOp.isGetDataByPullModeOperator()) {
			for (int i = 0; i < buffer.length ; i++) {
				tupleList.add(buffer[i]);
			}
		} else {			
			for (int i = 0; i < windowSize; i++) {
				tupleList.add(((TaggedTuple)sourceOperator.getNext()).getTuple());
			}
		}
		return tupleList;
	}	*/
	
	private List<Tuple> getTupleList() {
		List<Tuple> tupleList = new ArrayList<Tuple>();
		if (!windowOp.isGetDataByPullModeOperator()) {
			for (int i = 0; i < buffer.length; i++) {
				tupleList.add(buffer[i]);
			}
		} else {
			// Slide factor can be 0, +ve or -ve
			// If 0, there is no overlap and no need to store tuples for future
			// If +ve, store tuples that fall in the next slide, and use the
			// previous
			// tuple buffer to create the new one for window size
			// If -ve, there is a set of tuples that needs to be dropped in
			// between
			// two windows, just drop them
			int slideFactor = windowSize - slide;
			List<Tuple> newSlideTuples;// = new ArrayList<Tuple>(slideFactor);
			if (slideFactor > 0) {
				newSlideTuples = new ArrayList<Tuple>(slide);
			} else {
				newSlideTuples = new ArrayList<Tuple>(0);
			}
			// If slide factor is less than 0, then skip the next slideFactor
			// tuples
			// only if it is not the first window
			//FIXME This is just a crappy way to go about implementing
			//it in if else way, rather we need to bring in a timeout strategy
			//implementation. The If else is put in to avoid the thread getting
			//stuck. Not doing it due to lack of time
			if (!isFirst && slideFactor < 0) {
				for (int i = 0; i < slide; i++) {
					//FIXME This while true should be there, and used through
					//a receive time out. Otherwise, the thread will get stuck here
					//while (true) {
						//System.out.println("Stuck in here mate: ");
						TaggedTuple taggedTuple = (TaggedTuple) sourceOperator
								.getNext();
						//System.out
							//	.println("Stuck in here mate: " + taggedTuple);
						if (taggedTuple != null) {
							break;
						}
					//}
					System.out.println("Dropped " + i);
				}
				isFirst = true;
			} else {
				for (int i = 0; i < windowSize; i++) {

					Tuple nextTuple = null;
					if (slideFactor > 0
							&& (i < slide && prevSlideTuples.size() > 0)) {
						// If the slideFactor is greater than 0, then for the
						// size of the slide, get data from the previous tuple
						// list
						nextTuple = prevSlideTuples.get(i);
						//System.out.println("Here");
					} else {
						TaggedTuple taggedTuple = null;
						while (true) {
							//System.out.println("Stuck in here then?");
							taggedTuple = (TaggedTuple) sourceOperator
									.getNext();
							if (taggedTuple != null) {
								break;
							}
						}
						nextTuple = taggedTuple.getTuple();
					}
					tupleList.add(nextTuple);
					if (slideFactor > 0) {
						if (i >= slide) {
							newSlideTuples.add(nextTuple);
						}
					}

				}
				prevSlideTuples = newSlideTuples;
				if (slideFactor < 0) {
					isFirst = false;
				} else {
					isFirst = true;
				}
			}
		}
		return tupleList;
	}
	
	/*private List<Tuple> getTupleList2() {
		List<Tuple> tupleList = new ArrayList<Tuple>();
		if  (!windowOp.isGetDataByPullModeOperator()) {
			for (int i = 0; i < buffer.length ; i++) {
				tupleList.add(buffer[i]);
			}
		} else {
			//Slide factor can be 0, +ve or -ve
			//If 0, there is no overlap and no need to store tuples for future
			//If +ve, store tuples that fall in the next slide
			//If -ve, there is a set of tuples that needs to be dropped in between
			//two windows, just drop them
			int slideFactor = windowSize - slide;
			List<Tuple> newSlideTuples = new ArrayList<Tuple>(slide);
			for (int i = 0; i < windowSize; i++) {
				Tuple nextTuple = null;
				if (i < slide && prevSlideTuples.size() > 0) {					
					nextTuple = prevSlideTuples.get(i);					
				} else {
					TaggedTuple taggedTuple = null;
					while (true) {
						taggedTuple = (TaggedTuple)sourceOperator.getNext();
						if (taggedTuple != null) {
							break;
						}
					}
					nextTuple = taggedTuple.getTuple();
					
				}
				tupleList.add(nextTuple);
				if (slide > 0 && i >= slide) {
					newSlideTuples.add(nextTuple);
				}
			}
			prevSlideTuples = newSlideTuples;			
		}
		return tupleList;
	}*/

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
