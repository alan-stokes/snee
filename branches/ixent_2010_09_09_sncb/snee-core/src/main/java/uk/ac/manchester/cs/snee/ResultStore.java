package uk.ac.manchester.cs.snee;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.types.Duration;

public interface ResultStore {

	public void add(Output data);
	
	public void addAll(Collection<Output> data);
	
	/**
	 * Returns the current size of the result set
	 * @return size of the result set
	 */
	public int size();

	/**
	 * Retrieves the number, types and properties of this 
	 * <code>StreamResultSet</code> object's columns.
	 * 
	 * @return the description of this <code>StreamResultSet</code> 
	 * object's columns
	 */
	public ResultSetMetaData getMetadata();
	
	/**
	 * Retrieves this <code>StreamResultSet</code> object's command 
	 * property. The command property contains a command string, which 
	 * must be an SNEEql query, that can be executed to fill the 
	 * StreamResultSet with data. 
	 * The default value is <code>null</code>.
	 * 
	 * @return the command string; may be <code>null</code>
	 * @see StreamResultSet#setCommand(String);
	 */
	public String getCommand();
	
	/**
	 * Sets this <code>StreamResultSet</code> object's command 
	 * property to the given SNEEql query.
	 *  
	 * @param cmd the SNEEql query that will be used to get the data 
	 * for this <code>StreamResultSet</code> object; 
	 * may be <code>null</code>
	 * 
	 * @see ResultStore#getCommand(String)
	 */
	public void setCommand(String cmd);
	
	/**
	 * Return all of the results available for the specified query.
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResults()
	throws SNEEException;

	/**
	 * Return the specified number of results starting with the oldest available.
	 * 
	 * @param count The number of result items that should be returned
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException The count parameter extends beyond the number of available result items
	 */
	public List<ResultSet> getResults(int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting with the oldest available.
	 * @param duration
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResults(Duration duration) 
	throws SNEEException;

	/**
	 * Return all of the results starting from the specified index value.
	 * @param index  
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers A <code>ResultSet</code> containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromIndex(int index)
	throws SNEEException;
	
	/**
	 * Return the specified number of results starting from the index value.
	 * @param index
	 * @param count
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromIndex(int index, int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting 
	 * from the specified index value.
	 * @param index
	 * @param duration
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromIndex(int index, Duration duration) 
	throws SNEEException;

	/**
	 * Return all of the results starting from the specified timestamp.
	 * @param timestamp
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp) 
	throws SNEEException;

	/**
	 * Return the specified number of results starting from 
	 * the specified timestamp.
	 * @param timestamp
	 * @param count
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp, 
			int count) 
	throws SNEEException;

	/**
	 * Return the specified duration of results starting 
	 * from the specified timestamp.
	 * @param timestamp
	 * @param duration
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getResultsFromTimestamp(Timestamp timestamp, 
			Duration duration) 
	throws SNEEException;

	/**
	 * Return the result items.
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getNewestResults()
	throws SNEEException;
	
	/**
	 * Return the specified number of result items, counting back from the most recent.
	 * @param count
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getNewestResults(int count) 
	throws SNEEException;
	
	/**
	 * Return the specified duration of result items, spanning back in time from the
	 * most recent.
	 * @param duration
	 * 
	 * @return A <code>List</code> of <code>ResultSet</code> objects containing the specified answers
	 * @throws SNEEException
	 */
	public List<ResultSet> getNewestResults(Duration duration) 
	throws SNEEException;

}
