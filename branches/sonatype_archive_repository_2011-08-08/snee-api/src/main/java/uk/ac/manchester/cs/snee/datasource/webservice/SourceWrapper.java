package uk.ac.manchester.cs.snee.datasource.webservice;

import java.util.List;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public interface SourceWrapper {

	public List<String> getResourceNames() 
	throws SNEEDataSourceException;

	public List<ExtentMetadata> getSchema(String resourceName) 
	throws SNEEDataSourceException, SchemaMetadataException, 
		TypeMappingException;

	public List<Tuple> getData(String resourceName) 
	throws SNEEDataSourceException, TypeMappingException, 
		SchemaMetadataException, SNEEException;

	public SourceType getSourceType();

}
