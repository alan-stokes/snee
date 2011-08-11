package uk.ac.manchester.cs.snee.datasource.webservice;

import java.sql.Timestamp;
import java.util.List;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public interface PullSourceWrapper extends SourceWrapper {

	public List<Tuple> getData(String resourceName, int numItems, 
		Timestamp timestamp) 
	throws SNEEDataSourceException, TypeMappingException,
		SchemaMetadataException, SNEEException;
	
	public List<Tuple> getNewestData(String resourceName, 
		Integer numItems) 
	throws SNEEDataSourceException, TypeMappingException, 
		SchemaMetadataException, SNEEException;
	
}