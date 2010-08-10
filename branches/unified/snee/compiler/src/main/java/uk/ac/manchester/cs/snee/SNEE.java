package uk.ac.manchester.cs.snee;

import java.net.MalformedURLException;
import java.util.Collection;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSException;
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.evaluator.EvaluatorException;
import uk.ac.manchester.cs.snee.evaluator.StreamResultSet;

public interface SNEE {

	/**
	 * Adds a SSG4Env Pull Stream Interface web service source to the 
	 * set of available data for querying.
	 * @param url
	 * @throws MalformedURLException
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 * @throws SNEEDataSourceException
	 */
	public void addServiceSource(String url) 
	throws MalformedURLException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException;
	
	/**
	 * Return a list of the extent names available in the schema
	 * @return list of available extents
	 */
	public Collection<String> getExtents();
	
	/**
	 * Retrieve the metadata about a specified extent.
	 * @param extentName name of the extent
	 * @return details about the extent
	 * @throws ExtentDoesNotExistException name is not an extent in the schema
	 */
	public ExtentMetadata getExtentDetails(String extentName) 
	throws ExtentDoesNotExistException;
	
	/**
	 * Adds a query to the set of registered queries and returns the generated
	 * query identifier. 
	 * 
	 * It takes a query statement as input, generates a query plan for its 
	 * evaluation, and adds it to the set of registered query plans.
	 * 
	 * @param query Statement of the query
	 * @throws SNEEException
	 * @throws SchemaMetadataException 
	 * @throws EvaluatorException 
	 * @throws QoSException 
	 */
	public int addQuery(String query, String parametersFile) 
	throws SNEEException, SchemaMetadataException, EvaluatorException, QoSException;

	/**
	 * Retrieve the ResultSet for a specified query if it exists.
	 * @param queryId Identifier of the query for which the result set should be returned
	 * @return ResultSet for the query
	 * @throws SNEEException Specified queryId does not exist
	 */
	public StreamResultSet getResultSet(int queryId) 
	throws SNEEException;
	
	/**
	 * Removes a query from the set of registered queries. It takes a query 
	 * identifier and stops the required query evaluation if it exists.
	 * 
	 * @param queryId Identifier of the query to be stopped
	 * @throws SNEEException An exception is thrown if the query identifier does not exist
	 */
	public void removeQuery(int queryId) 
	throws SNEEException;

	/**
	 * Close SNEE down gracefully
	 */
	public void close();

}