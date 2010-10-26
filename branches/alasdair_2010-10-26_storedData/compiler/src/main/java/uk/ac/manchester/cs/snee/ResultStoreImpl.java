package uk.ac.manchester.cs.snee;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Observable;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryPlanMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.types.Duration;

public class ResultStoreImpl 
extends Observable implements ResultStore {
	
	private static Logger logger = 
		Logger.getLogger(ResultStoreImpl.class.getName());

	private CircularArray<Output> _data;

	private String command = null;

	private ResultSetMetaData metadata;

	private String queryId;

	public ResultStoreImpl(String query, QueryExecutionPlan queryPlan) 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER ResultStoreImpl() for " + query);
		try {
			queryId = setQueryId(queryPlan);
			command = query;
			metadata = createMetaData(queryPlan);
			_data = createDataStore();
		} catch (SQLException e) {
			String message = "Problems generating metadata.";
			logger.warn(message, e);
			throw new SNEEException(message);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN ResultStoreImpl()");
	}

	protected String setQueryId(QueryExecutionPlan queryPlan) {
		return queryPlan.getID();
	}

	protected ResultSetMetaData createMetaData(
			QueryExecutionPlan queryPlan)
	throws SQLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createMetaData() with " + 
					queryPlan);
		}
		QueryPlanMetadata queryMetaData = queryPlan.getMetaData();
		RowSetMetaData md = new RowSetMetaDataImpl();
		List<Attribute> attributes = 
			queryMetaData.getOutputAttributes();
		md.setColumnCount(attributes.size());
		int columnIndex = 0;
		if (logger.isTraceEnabled()) {
			logger.trace("Number of attributes: " + 
					attributes.size());
		}
		for (Attribute attr : attributes) {
			columnIndex++;
			if (logger.isTraceEnabled()) {
				logger.trace("Attribute " + columnIndex + ": " +
						attr);
			}
			md.setAutoIncrement(columnIndex, false);
			md.setCaseSensitive(columnIndex, false);
			md.setCatalogName(columnIndex, attr.getExtentName());
			md.setColumnName(columnIndex, 
					attr.getAttributeSchemaName());
			md.setColumnLabel(columnIndex, 
					attr.getAttributeDisplayName());
			md.setColumnType(columnIndex, 
					attr.getAttributeType());
			md.setColumnTypeName(columnIndex, 
					attr.getAttributeTypeName());
			md.setCurrency(columnIndex, false);
			md.setNullable(columnIndex,
					ResultSetMetaData.columnNoNulls);
			md.setTableName(columnIndex, attr.getExtentName());
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createMetaData() #columns=" +
					md.getColumnCount());
		}
		return md;
	}

	protected CircularArray<Output> createDataStore() 
	throws SNEEConfigurationException {
		int maxBufferSize = SNEEProperties.getIntSetting(
				SNEEPropertyNames.RESULTS_HISTORY_SIZE_TUPLES);
		if (logger.isTraceEnabled()) {
			logger.trace("Buffer size: " + maxBufferSize);
		}
		return new CircularArray<Output>(maxBufferSize);
	}
	
	public void add(Output data) {
		if (logger.isDebugEnabled()) {
			logger.debug("Query " + queryId + " Number of " +
					"existing data items: " + _data.size());
		}
		synchronized (this) {			
			_data.add(data);
		}
		if (logger.isTraceEnabled())
			logger.trace("Notifying subscribe clients. " + data);
		setChanged();
		try {
			List<Output> dataList = new ArrayList<Output>();
			dataList.add(data);
			notifyObservers(createResultSets(dataList));
		} catch (SNEEException e) {
			logger.error("Problem notifying consumers. " + e);
		}
	}
	
	public void addAll(Collection<Output> data) {
		if (logger.isDebugEnabled()) {
			logger.debug("Query " + queryId + " Number of " +
					"existing data items: " + _data.size());
		}
		synchronized (this) {
			_data.addAll(data);
		}
		if (logger.isTraceEnabled())
			logger.trace("Notifying subscribe clients. " + data.size());
		setChanged();
		try {
			notifyObservers(createResultSets((List<Output>) data));
		} catch (SNEEException e) {
			logger.error("Problem notifying consumers. " + e);
		}
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
			String msg = "Count (" + count +
				") is less than or equal " + "to 0.";
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
			logger.trace("ENTER checkDuration() with duration " + 
					duration);
		}
		if (duration.getDuration() <= 0) { 
			String msg = "Duration (" + duration + 
				") is less than or " +
			"equal to 0.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}

		long durationPeriod = duration.getDuration();		
		Output firstResult = _data.getOldest();
		long oldestTS = firstResult.getEvalTime();
		Output newestResult = _data.getNewest();
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
		if (timestamp.after(new Timestamp(System.currentTimeMillis()))) 
		{
			String msg = "For retrieving results, " +
					"the timestamp must be in the past.";
			logger.warn(msg);
			throw new SNEEException(msg);
		} 
		
		Timestamp oldestTS = new Timestamp(_data.getOldest().getEvalTime());
		if (timestamp.before(oldestTS)) {
			String msg = "The oldest timestamp available is " + 
				oldestTS;
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
				logger.trace("Using 'now' as max timestamp: " +
						maxTimestamp);
		}
		List<Output> response = new ArrayList<Output>();
		int count = 0;
		for (Output output : _data) {
			Timestamp outputTS = new Timestamp(output.getEvalTime());
			/* 
			 * Check that item's timestamp is newer than required 
			 * timestamp and less than the maximum timestamp 
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
			logger.trace("RETURN generateTimestampResultSet(), " +
					"number " + "of results: " + response.size());
		}
		return response;
	}

	private List<ResultSet> createResultSets(
			CircularArray<Output> outputs) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createResultSets(CircularArray)");
		}
		List<Output> dataList = new ArrayList<Output>();
		for (Output output : outputs) {
			dataList.add(output);
		}
		List<ResultSet> resultList = createResultSets(dataList);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createResultSets() #resultSets=" +
					resultList.size());
		}
		return resultList;
	}
	
	private List<ResultSet> createResultSets(List<Output> outputs)
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createResultSets()");
		}
		Output output = outputs.get(0);
		List<ResultSet> resultSets = new ArrayList<ResultSet>();
		try {
			if (output instanceof TaggedTuple) {
				if (logger.isTraceEnabled()) {
					logger.trace("Processing stream of tuples");
				}
				List<Tuple> results = new ArrayList<Tuple>();
				for (Output result : outputs) {
					TaggedTuple tt = (TaggedTuple) result; 
					Tuple tuple = tt.getTuple();
					results.add(tuple);
				}
				//XXX: Need to decide if we will use the override method
				ResultSet resultSet = 
					new StreamResultSet(metadata, results);
				resultSets.add(resultSet);
			} else if (output instanceof Window) {
				if (logger.isTraceEnabled()) {
					logger.trace("Processing stream of windows");
				}
				for (Output result : outputs) {
					Window win = (Window) result;
					logger.trace("Window: " + win);
					//XXX: ResultSet creation overridden for tests!
					ResultSet rs = createRS(win.getTuples());
					resultSets.add(rs);
				}
			} else {
				String message = output.getClass() + 
				" Unsupported output type at this time.";
				logger.warn(message);
				throw new SNEEException(message);
			}
		} catch (SQLException e) {
			String message = "Problem creating ResultSet object.";
			logger.warn(message, e);
			throw new SNEEException(message);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createResultSets() #resultSets=" +
					resultSets.size());
		}
		return resultSets;
	}

	protected ResultSet createRS(List<Tuple> tuples) 
	throws SQLException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createRS()");
		}
		ResultSet rs = new StreamResultSet(metadata, tuples);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createRS()");
		}
		return rs;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getNewestResults()
	 */
	public List<ResultSet> getNewestResults() 
	throws SNEEException {
		/* Equivalent to calling getResults(int queryId) */
		return getResults();
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getNewestResults(int)
	 */
	public List<ResultSet> getNewestResults(int count)
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getNewestResults() with count " + count);
		}
		
		checkCount(count);

		int toIndex = _data.size();
		List<Output> output = 
			_data.subList(toIndex - count, toIndex);
		List<ResultSet> resultSets = createResultSets(output);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getNewestResults() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}


	public List<ResultSet> getNewestResults(Duration duration)
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
		long availableDuration = newestTS - _data.getOldest().getEvalTime();
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
		List<ResultSet> resultSets = createResultSets(response);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getNewestResults()  #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults()
	 */
	public List<ResultSet> getResults() throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResults()");
		}
		List<ResultSet> resultSets = createResultSets(_data);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResults()  #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults(int)
	 */
	public List<ResultSet> getResults(int count) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResults() with count=" + count);
		}

		checkCount(count);

		List<Output> output = _data.subList(0, count);
		List<ResultSet> resultSets = createResultSets(output);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResults() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResults(uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<ResultSet> getResults(Duration duration)
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
		List<ResultSet> resultSets = createResultSets(response);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResult() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int)
	 */
	public List<ResultSet> getResultsFromIndex(int index)
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultFromIndex() with index " + 
					index);
		}
		checkIndex(index);
		int toIndex = _data.size();
		List<Output> output = 
			_data.subList(index, toIndex); 
		List<ResultSet> resultSets = createResultSets(output);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultFromIndex() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int, int)
	 */
	public List<ResultSet> getResultsFromIndex(int index, int count) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultFromIndex() with index " + 
					index + ", count " + count);
		}
		checkCount(count);
		checkIndex(index);
		int toIndex = index + count; 
		if (toIndex > _data.size()) {
			String msg = "Trying to retrieve too many items (" + 
				count + "). Number of available data items " + 
				_data.size();
			logger.warn(msg);
			throw new SNEEException(msg);
		}		
		List<Output> output = 
			_data.subList(index, toIndex); 
		List<ResultSet> resultSets = createResultSets(output);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultFromIndex() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromIndex(int, uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<ResultSet> getResultsFromIndex(int index, 
			Duration duration) 
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
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultsFromTimestamp() with " +
					"timestamp " + timestamp);
		}		
		checkTimestamp(timestamp);
		
		List<Output> response = 
			generateTimestampResultSet(timestamp, _data.size(), null);
		List<ResultSet> resultSets = createResultSets(response);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromTimestamp(java.sql.Timestamp, int)
	 */
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp, 
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
		List<ResultSet> resultSets = createResultSets(response);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.StreamResultSet#getResultsFromTimestamp(java.sql.Timestamp, uk.ac.manchester.cs.snee.types.Duration)
	 */
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp, 
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
		List<ResultSet> resultSets = createResultSets(response);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN getResultsFromTimestamp() #resultSets " + 
    				resultSets.size());
    	}
		return resultSets;
	}

	@Override
	public ResultSetMetaData getMetadata() {
		return metadata;
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
