package uk.ac.manchester.cs.snee.data.webservice;

import java.util.List;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.Attribute;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

public interface SchemaParser {

	public String getExtentName() 
	throws SchemaMetadataException;
	
	public List<Attribute> getColumns(String extentName) 
	throws TypeMappingException, SchemaMetadataException;
	
}
