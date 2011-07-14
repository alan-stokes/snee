package uk.ac.manchester.cs.snee.datasource.webservice;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.sql.rowset.WebRowSet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

import com.sun.rowset.WebRowSetImpl;

public abstract class SourceWrapperAbstract implements SourceWrapper {

	private static Logger logger = 
		Logger.getLogger(SourceWrapperAbstract.class.getName());
	
	protected static final String DATASET_FORMAT = 
		"http://java.sun.com/xml/ns/jdbc";

	protected SourceType _sourceType;
	
	protected String _url;

	protected Types _types;

	private SQLUtils _sqlUtils;

    public SourceWrapperAbstract(String url, Types types) 
    throws MalformedURLException {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER SourceWrapperAbstract() with URL " + url);
    	_types = types;
    	_url = url;
    	_sqlUtils = new SQLUtils(types);
        if (logger.isDebugEnabled())
        	logger.debug("RETURN SourceWrapperAbstract()");
    }
    
	@Override
	public SourceType getSourceType() {
    	return _sourceType;
    }
    
	protected List<ExtentMetadata> extractSchema(SchemaParser schemaParser,
			ExtentType extentType) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER extractSchema()");
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		Collection<String> extentNames = schemaParser.getExtentNames();
		for (String extentName : extentNames) {
			List<Attribute> attributes = 
				schemaParser.getColumns(extentName);
			ExtentMetadata extentMetadata = 
				new ExtentMetadata(extentName, attributes, extentType, null);
			extents.add(extentMetadata);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN extractSchema() #extents=" + 
					extents.size());
		return extents;
	}

	protected List<Tuple> processWRSDataset(String data)
	throws TypeMappingException, SchemaMetadataException,
			SNEEDataSourceException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWRSDataset(String WebRowSet) with " + 
					data);
		}
		List<Tuple> tuples;
		try {
			WebRowSet wrs = new WebRowSetImpl();
			wrs.readXml(new StringReader(data));
			if (logger.isTraceEnabled())
				logger.trace("Successfully read in WebRowSet. " +
						"Number of rows " + wrs.size());
			tuples = processWRS(wrs);
		} catch (SQLException e) {
			String msg = "Problem reading in WebRowSet. " + e;
			logger.warn(msg);
			throw new SNEEDataSourceException(msg, e);		
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWRSDataset(String WebRowSet), " +
					"number of tuples " + tuples.size());
		}
		return tuples;
	}

	protected List<Tuple> processWRS(WebRowSet wrs) 
	throws TypeMappingException, SchemaMetadataException,
	SNEEDataSourceException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processWRS(WebRowSet) with " + wrs);
		}
		List<Tuple> tuples = new ArrayList<Tuple>();
		//Retrieve resultset metadata
		try {
			ResultSetMetaData wrsMetadata = wrs.getMetaData();
			int numberColumns = wrsMetadata.getColumnCount();
			//Move pointer to before first tuple
			wrs.beforeFirst();
			//Loop through webrowset
			while (wrs.next()) {
				//Retrieve the next row from the webrowset
				int rowNumber = wrs.getRow();
				//Create tuple
				Tuple tuple = new Tuple();
				for (int i = 1; i <= numberColumns; i++) {
					//Populate fields of tuple
					String attrName = wrsMetadata.getColumnLabel(i);
					String extentName = wrsMetadata.getTableName(i);
					AttributeType dataType = 
						_sqlUtils.inferType(wrsMetadata.getColumnType(i));
					Object value = wrs.getObject(i);
					EvaluatorAttribute attr = 
						new EvaluatorAttribute(extentName, attrName, 
								dataType, value);
					if (logger.isTraceEnabled()) {
						logger.trace("Received attribute: " + attr);
					}
					tuple.addAttribute(attr);
				}
				//Add tuple to tuple set
				tuples.add(tuple);
			}
		} catch (SQLException e) {
			String msg = "Problem with WebRowSet. " + e;
			logger.warn(msg);
			throw new SNEEDataSourceException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processWRS(WebRowSet), " +
					"number of tuples " + tuples.size());
		}
		return tuples;
	}
    
}