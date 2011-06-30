package uk.ac.manchester.cs.snee.datasource.webservice;

import java.sql.Timestamp;
import java.util.List;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public interface PullSourceWrapper extends SourceWrapper {

	/**
	 * Retrieve data for a given {@code PullSource} resource from a specified 
	 * point of time ranging forward a specified number of tuples.
	 * 
	 * @param resourceName name of the resource to retrieve values from
	 * @param numItems number of items to retrieve
	 * @param timestamp point in time from which to gather tuples
	 * @return A tuple list with the returned data values
	 * @throws SNEEDataSourceException
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEException
	 */
	public List<Tuple> getData(String resourceName, int numItems, 
		Timestamp timestamp) 
	throws SNEEDataSourceException, TypeMappingException,
		SchemaMetadataException, SNEEException;
	
	/**
	 * Retrieve the specified number of newest data items for a specified 
	 * {@code PullSource} resource
	 * 
	 * @param resourceName name of the resource to retrieve values from
	 * @param numItems number of items to retrieve
	 * @return
	 * @throws SNEEDataSourceException
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEException
	 */
	public List<Tuple> getNewestData(String resourceName, 
		Integer numItems) 
	throws SNEEDataSourceException, TypeMappingException, 
		SchemaMetadataException, SNEEException;
	
}