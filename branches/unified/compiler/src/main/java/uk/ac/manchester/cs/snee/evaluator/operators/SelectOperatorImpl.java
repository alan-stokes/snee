package uk.ac.manchester.cs.snee.evaluator.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.Operator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.SelectOperator;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;

public class SelectOperatorImpl extends EvaluationOperator {
	//XXX: Write test for select operator
	
	Logger logger = Logger.getLogger(this.getClass().getName());

	private SelectOperator _select;

	public SelectOperatorImpl(Operator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SelectOperatorImpl " + op);
		}
		
		// Instantiate the select condition
		_select = (SelectOperator) op;
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SelectOperatorImpl()");
		}
	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() with " + observed);
		try {
			//FIXME: Cope with bag of results being passed up!
			List<Output> result = new ArrayList<Output>();
			if (observed instanceof List<?>) {
				for (Object ob : (List)observed) {
					processOutput(ob, result);
				}
			} else if (observed instanceof Output) {
				processOutput(observed, result);
			} else {
				String msg = "Unknown item type (" +
				observed.getClass().getSimpleName() + ") in stream.";
				logger.warn(msg);
				throw new SNEEException(msg);
			}
			if (result != null) {
				if (logger.isTraceEnabled())
					logger.trace("Notify consumers");
				setChanged();
				notifyObservers(result);
			}
		} catch (Exception e) {
			logger.error(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}

	private void processOutput(Object observed, List<Output> result)
	throws SNEEException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processOutput()  with " + observed);
		if (observed instanceof Window) {
			Window window = (Window) observed;
			List<Tuple> selectedTuples = new ArrayList<Tuple>();
			for (Tuple t : window.getTuples()){
				boolean valid = 
					evaluate((MultiExpression)_select.getPredicate(), t);
				if (logger.isTraceEnabled()) {
					logger.trace("Select " + t + " " + valid);
				}
				if (valid){
					selectedTuples.add(t);
				}
			}
			// Create new window
			Window newWindow = new Window(selectedTuples);
			if (logger.isTraceEnabled()) {
				logger.debug("Window after select op: " + newWindow);
			}
			result.add(newWindow);
		} else if (observed instanceof TaggedTuple) {				
			TaggedTuple tuple = (TaggedTuple) observed;
			boolean valid = 
				evaluate((MultiExpression)_select.getPredicate(), 
						tuple.getTuple());
			if (logger.isTraceEnabled()) {
				logger.trace("Select " + tuple + " " + valid);
			}
			if (valid){
				result.add(tuple);
			} 
		} else {
			String msg = "Unknown item type (" +
				observed.getClass().getSimpleName() + ") in stream.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processOutput()");
	}

//	@Override
//	public Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException{
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER getNext()");
//		}
//		// Create collection for operator results
//		List<Output> resultItems = new ArrayList<Output>();
//		
//		// Get bag of stream items from child operator
//		Collection<Output> bagOfItems = child.getNext();
//
//		// Iterate over the items received from the child operator
//		for (Output item : bagOfItems) {
//			if (item instanceof Window) {
//				//XXX: Attention: What are the evaluation semantics of a select over a window?
//				Window window = (Window) item;
//				List<Tuple> selectedTuples = new ArrayList<Tuple>();
//				for (Tuple t : window.getTuples()){
//					boolean valid = 
//						evaluate((MultiExpression)_select.getPredicate(), t);
//					if (logger.isTraceEnabled()) {
//						logger.trace("Select " + t + " " + valid);
//					}
//					if (valid){
//						selectedTuples.add(t);
//					}
//				}
//				// Create new window
//				Window newWindow = new Window(selectedTuples);
//				if (logger.isTraceEnabled()) {
//					logger.debug("Window after select op: " + newWindow);
//				}
//				resultItems.add(newWindow);
//			} else if (item instanceof TaggedTuple) {				
//				TaggedTuple tuple = (TaggedTuple) item;
//				boolean valid = 
//					evaluate((MultiExpression)_select.getPredicate(), 
//							tuple.getTuple());
//				if (logger.isTraceEnabled()) {
//					logger.trace("Select " + tuple + " " + valid);
//				}
//				if (valid){
//					resultItems.add(tuple);
//				} 
//			} else {
//				String msg = "Unknown item type (" +
//					item.getClass().getSimpleName() + ") in stream.";
//				logger.warn(msg);
//				throw new SNEEException(msg);
//			}
//		}
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN getNext() number of stream items " + 
//					resultItems.size());
//		}
//		return resultItems;
//	}

}
