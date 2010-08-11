package uk.ac.manchester.cs.snee.evaluator;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.types.Duration;

//FIXME: Make StreamResultSet extend Observable!
public interface StreamResultSet {

	public void add(Output data);
	
	public void addAll(Collection<Output> data);

	public List<String> getAttributes();

	public void setAttributes(List<String> attributes);
	
	/**
	 * Returns the current size of the result set
	 * @return size of the result set
	 */
	public int size();

	/**
	 * Return all of the results available for the specified query.
	 * 
	 * @return A list containing all the query answers
	 * @throws SNEEException
	 */
	public List<Output> getResults()
	throws SNEEException;

	/**
	 * Return the specified number of results starting with the oldest available.
	 * 
	 * @param count The number of result items that should be returned
	 * 
	 * @return A list containing the specified number of answers
	 * @throws SNEEException The count parameter extends beyond the number of available result items
	 */
	public List<Output> getResults(int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting with the oldest available.
	 * @param duration
	 * 
	 * @return A list containing the answers for the specified duration
	 * @throws SNEEException
	 */
	public List<Output> getResults(Duration duration) 
	throws SNEEException;

	/**
	 * Return all of the results starting from the specified index value.
	 * @param index  
	 * 
	 * @return A list containing the answers from the specified index value
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromIndex(int index)
	throws SNEEException;
	
	/**
	 * Return the specified number of results starting from the index value.
	 * @param index
	 * @param count
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromIndex(int index, int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting 
	 * from the specified index value.
	 * @param index
	 * @param duration
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromIndex(int index, Duration duration) 
	throws SNEEException;

	/**
	 * Return all of the results starting from the specified timestamp.
	 * @param timestamp
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp) 
	throws SNEEException;

	/**
	 * Return the specified number of results starting from 
	 * the specified timestamp.
	 * @param timestamp
	 * @param count
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp, 
			int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting 
	 * from the specified timestamp.
	 * @param timestamp
	 * @param duration
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getResultsFromTimestamp(Timestamp timestamp, 
			Duration duration) 
	throws SNEEException;

	/**
	 * Return the result items.
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getNewestResults()
	throws SNEEException;
	
	/**
	 * Return the specified number of result items, counting back from the most recent.
	 * @param count
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getNewestResults(int count) 
	throws SNEEException;
	
	/**
	 * Return the specified duration of result items, spanning back in time from the
	 * most recent.
	 * @param duration
	 * 
	 * @return A list containing the answers
	 * @throws SNEEException
	 */
	public List<Output> getNewestResults(Duration duration) 
	throws SNEEException;

}
