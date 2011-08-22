package uk.ac.manchester.cs.snee.datasource.webservice;

import java.net.MalformedURLException;
import java.util.List;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;

public abstract class StoredSourceWrapperAbstract extends SourceWrapperAbstract
implements SourceWrapper {

    public StoredSourceWrapperAbstract(String url, Types types) 
    throws MalformedURLException {
    	super(url, types);
    }

    public abstract List<Tuple> executeQuery(String resourceName, String sqlQuery) 
    throws SNEEDataSourceException, TypeMappingException, SchemaMetadataException, 
    SNEEException;
}