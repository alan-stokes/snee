package uk.ac.manchester.cs.snee;

import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.types.Duration;

public class StreamResultSetImpl 
extends Observable implements StreamResultSet {
	
	private static Logger logger = 
		Logger.getLogger(StreamResultSetImpl.class.getName());

	private List<Output> _data;

	private List<String> _attributes;

	private String command = null;

	public StreamResultSetImpl() {//(String query) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER StreamResultSetImpl()");// with " + query);		
		_attributes = new ArrayList<String>();
		_data = createDataStore();
//		command = query;
		if (logger.isDebugEnabled())
			logger.debug("RETURN StreamResultSetImpl()");
	}

	protected List<Output> createDataStore() {
		return new ArrayList<Output>();
	}

	public List<String> getAttributes() {
		return _attributes;
	}

	public void setAttributes(List<String> attributes) {
		_attributes = attributes;
	}
	
	public void add(Output data) {
		synchronized (this) {			
			_data.add(data);
		}
		if (logger.isTraceEnabled())
			logger.trace("Notifying subscribe clients. " + data);
		setChanged();
		notifyObservers(data);
	}
	
	public void addAll(Collection<Output> data) {
		synchronized (this) {
			_data.addAll(data);
		}
		if (logger.isTraceEnabled())
			logger.trace("Notifying subscribe clients. " + data.size());
		setChanged();
		notifyObservers(data);
	}	
	
	public int size() {
		return _data.size();
	}

	private void checkCount(int count) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER checkCount() with count " + count);
		}
		if (count <= 0) { 
			String msg = "Count (" + count + ") is less than or equal " +
					"to 0.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (count > _data.size()) {
			String msg = "Count (" + count + 
				") larger than number of available result items (" + 
				_data.size() + ").";
			logger.warn(msg);
			throw new SNEEException(msg);	
		}
		logger.trace("RETURN checkCount()");
	}

	private void checkIndex(long index) 
	throws SNEEException
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER checkIndex() with index " + index);
		if (index < 0) {
			String msg = "Lowest valid index is 0.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (index >= _data.size()) {
			int maxIndex = _data.size() - 1;
			String msg = "Largest valid index is " + maxIndex  + ".";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN checkIndex()");
	}

	private void checkDuration(Duration duration) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER checkDuration() with duration " + duration);
		}
		if (duration.getDuration() <= 0) { 
			String msg = "Duration (" + duration + ") is less than or " +
			"equal to 0.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}

		long durationPeriod = duration.getDuration();		
		Output firstResult = _data.get(0);
		long oldestTS = firstResult.getEvalTime();
		Output newestResult = _data.get(_data.size()-1);
		long newestTS = newestResult.getEvalTime();
		long availableDuration = newestTS - oldestTS;
		
		if (availableDuration < durationPeriod) {
			String msg = "Requested duration (" + durationPeriod + 
			") is greater than available duration (" + 
			availableDuration + ").";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN checkDuration()");
	}

	private void checkTimestamp(Timestamp timestamp) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER checkTimestamp() with timestamp " + 
					timestamp);
		}
		if (timestamp.after(new Timestamp(System.currentTimeMillis()))) {
			String msg = "For retrieving results, the timestamp must be in the past.";
			logger.warn(msg);
			throw new SNEEException(msg);
		} 
		
		Timestamp oldestTS = new Timestamp(_data.get(0).getEvalTime());
		if (timestamp.before(oldestTS)) {
			String msg = "The oldest timestamp available is " + oldestTS;
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		logger.trace("RETURN checkTimestamp()");
	}

	private List<Output> generateTimestampResultSet(Timestamp timestamp, 
			int maxNumber, Timestamp maxTimestamp) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER generateTimestampResultSet() with " +
					"timestamp: " + timestamp + 
					" max number of results " + maxNumber +
					" max timestamp "+ maxTimestamp);
		}
		if (maxTimestamp == null) {
			maxTimestamp = new Timestamp(System.currentTimeMillis());
			if (logger.isTraceEnabled())
				logger.trace("Using 'now' as max timestamp: " + maxTimestamp);
		}
		List<Output> response = new ArrayList<Output>();
		int count = 0;
		for (Output output : _data) {
			Timestamp outputTS = new Timestamp(output.getEvalTime());
			/* 
			 * Check that item's timestamp is newer than required timestamp
			 * and less than the maximum timestamp 
			 */
			if (outputTS.compareTo(timestamp) >= 0 && 
					outputTS.before(maxTimestamp)) {
				if (logger.isTraceEnabled())
					logger.trace("Adding " + output + " to response.");
				response.add(output);
				count++;
				if (count == maxNumber) {
					if (logger.isTraceEnabled())
						logger.trace("No more results need to be added");
					break;
				}
					
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN generateTimestampResultSet(), number " +
					"of results: " + response.size());
		}
		return response;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getNewestResults()
	 */
	public List<Output> getNewestResults() 
	throws SNEEException {
		/* Equivalent to calling getResults(int queryId) */
		return getResults();
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getNewestResults(int)
	 */
	public List<Output> getNewestResults(int count)
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getNewestResults() with count " + count);
		}
		
		checkCount(count);

		int toIndex = _data.size();
		List<Output> output = 
			_data.subList(toIndex - count, toIndex);
		
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getNewestResults() result size " + 
    				output.size());
    	}
		return output;
	}


	public List<Output> getNewestResults(Duration duration)
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getNewestResults() with duration " + 
					duration);
		}
		if (duration==null) {
			String msg = "Null duration, assuming getResults()";
			logger.warn(msg);
			if (logger.isDebugEnabled())
				logger.debug("RETURN getNewestResutls()");
			return getResults();
		} else if (duration.getDuration() <= 0) { 
			String msg = "Duration (" + duration + ") is less than or " +
					"equal to 0.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}

		List<Output> response = new ArrayList<Output>();
		
		long durationPeriod = duration.getDuration();
		long newestTS = System.currentTimeMillis();
		long availableDuration = newestTS - _data.get(0).getEvalTime();
		if (availableDuration < durationPeriod) {
			String msg = "Requested duration (" + durationPeriod + 
				") is greater than available duration (" + 
				availableDuration + ").";
			logger.warn(msg);
			throw new SNEEException(msg);
		} else {
			for (int i = _data.size() - 1; i >= 0; i--) {
				long rangeMin = newestTS - durationPeriod;
				if (logger.isTraceEnabled())
					logger.trace("NewestTS: " + newestTS +
							"\tRangeMin: " + rangeMin);
				Output output = _data.get(i);
				if (output.getEvalTime() > rangeMin) {
					logger.trace("Adding output to response.");
					response.add(output);
				} else {
					if (logger.isTraceEnabled())
						logger.trace("No more results to add.");
					break;
				}
			}
		}		
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getNewestResults() result size " + 
    				response.size());
    	}
		return response;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults()
	 */
	public List<Output> getResults() throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResult()");
		}
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResult() result size " + _data.size());
    	}
		return _data;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults(int)
	 */
	public List<Output> getResults(int count) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResults() with count=" + count);
		}

		checkCount(count);

		List<Output> output = _data.subList(0, count);
		
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResults() result size " + output.size());
    	}
		return output;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults(uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<Output> getResults(Duration duration)
			throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResult() with duration " + duration);
		}
		
		if (duration==null) {
			String msg = "Null duration, assuming getResults()";
			logger.warn(msg);
			if (logger.isDebugEnabled())
				logger.debug("RETURN getResults()");
			return getResults();
		}

		checkDuration(duration);
		
		List<Output> response = new ArrayList<Output>();
		
		//XXX-AG: Duplication of calculation of oldest ts
		Output firstResult = _data.get(0);
		long oldestTS = firstResult.getEvalTime();
		long rangeMax = oldestTS + duration.getDuration();
		if (logger.isTraceEnabled())
			logger.trace("Oldest TS: " + oldestTS + 
					"\tRangeMax: " + rangeMax);
		for (Output output : _data) {
			long ts = output.getEvalTime();
			if (logger.isTraceEnabled()) {
				logger.trace("Output TS: " + ts + " " + output);
			}
			if (ts < rangeMax) {
				logger.trace("Adding output to response.");
				response.add(output);
			} else {
				if (logger.isTraceEnabled())
					logger.trace("No more results to add.");
				break;
			}
		}
		
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResult() result size " + response.size());
    	}
		return response;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int)
	 */
	public List<Output> getResultsFromIndex(int index)
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultFromIndex() with index " + index);
		}
		checkIndex(index);
		int toIndex = _data.size();
		List<Output> output = 
			_data.subList(index, toIndex); 
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultFromIndex() result size " + 
    				output.size());
    	}
		return output;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int, int)
	 */
	public List<Output> getResultsFromIndex(int index, int count) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultFromIndex() with index " + 
					index + ", count " + count);
		}
		checkCount(count);
		checkIndex(index);
		int toIndex = index + count; 
		if (toIndex >= _data.size()) {
			String msg = "Trying to retrieve to many items.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}		
		List<Output> output = 
			_data.subList(index, toIndex); 
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultFromIndex() result size " + 
    				output.size());
    	}
		return output;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int, uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<Output> getResultsFromIndex(int index, Duration duration) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultFromIndex() with index " + 
					index + ", duration " + duration);
		}
		throw new SNEEException("Method not implemented yet!");
//		List<Output> output = new ArrayList<Output>();
//		if (logger.isDebugEnabled()) {
//    		logger.debug("RETURN getResultFromIndex() result size " + 
//    				output.size());
//    	}
//		return output;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromTimestamp(java.sql.Timestamp)
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultsFromTimestamp() with " +
					"timestamp " + timestamp);
		}		
		checkTimestamp(timestamp);
		
		List<Output> response = 
			generateTimestampResultSet(timestamp, _data.size(), null);
		
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() result size " +
    				response.size());
    	}
		return response;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromTimestamp(java.sql.Timestamp, int)
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp, 
			int count) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultsFromTimestamp() with " +
					"timestamp " + timestamp +
					", count " + count);
		}
		checkCount(count);
		checkTimestamp(timestamp);
		/* Generate results */
		List<Output> response = 
			generateTimestampResultSet(timestamp, count, null);

		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() result size " + 
    				response.size());
    	}
		return response;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromTimestamp(java.sql.Timestamp, uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp, 
			Duration duration) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultsFromTimestamp() with " +
					"timestamp " + timestamp +
					", duration " + duration);
		}
		checkTimestamp(timestamp);
		checkDuration(duration);
		int maxSize = _data.size();
		Timestamp maxTimestamp = 
			new Timestamp(timestamp.getTime() + duration.getDuration());
		List<Output> response = 
			generateTimestampResultSet(timestamp, maxSize, maxTimestamp);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() result size " + 
    				response.size());
    	}
		return response;
	}

	@Override
	public ResultSetMetaData getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCommand() {
		return command;
	}

	@Override
	public void setCommand(String cmd) {
		command = cmd;
	}

}
