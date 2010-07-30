package uk.ac.manchester.cs.snee.data.webservice;

import java.util.Map;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

public interface SchemaParser {

	public String getExtentName() 
	throws SchemaMetadataException;
	
	public Map<String, AttributeType> getColumns() 
	throws TypeMappingException, SchemaMetadataException;
	
}
