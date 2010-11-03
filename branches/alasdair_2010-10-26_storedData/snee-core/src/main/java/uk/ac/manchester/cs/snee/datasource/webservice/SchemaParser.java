package uk.ac.manchester.cs.snee.datasource.webservice;

import java.util.Collection;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;

public interface SchemaParser {

	public Collection<String> getExtentNames() 
	throws SchemaMetadataException;
	
	public List<Attribute> getColumns(String extentName) 
	throws TypeMappingException, SchemaMetadataException;
	
}
