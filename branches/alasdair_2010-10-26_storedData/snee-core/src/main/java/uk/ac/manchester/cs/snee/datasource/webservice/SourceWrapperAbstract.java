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
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

import com.sun.rowset.WebRowSetImpl;

import eu.semsorgrid4env.service.wsdai.DataResourceAddressType;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public abstract class SourceWrapperAbstract {

	private static Logger logger = 
		Logger.getLogger(SourceWrapperAbstract.class.getName());
	
	protected static final String DATASET_FORMAT = 
		"http://java.sun.com/xml/ns/jdbc";

	protected String _url;

	protected Types _types;

    public SourceWrapperAbstract(String url, Types types) 
    throws MalformedURLException {
    	if (logger.isDebugEnabled())
    		logger.debug("ENTER SourceWrapperAbstract() with URL " + url);
    	_types = types;
    	_url = url;
        if (logger.isDebugEnabled())
        	logger.debug("RETURN SourceWrapperAbstract()");
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
				new ExtentMetadata(extentName, attributes, extentType);
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
		if (logger.isTraceEnabled())
			logger.trace("ENTER processWRSDataset() with " + data);
		List<Tuple> tuples = new ArrayList<Tuple>();
		try {
			WebRowSet wrs = new WebRowSetImpl();
			wrs.readXml(new StringReader(data));
			if (logger.isTraceEnabled())
				logger.trace("Successfully read in WebRowSet. " +
						"Number of rows " + wrs.size());
			//Retrieve resultset metadata
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
					AttributeType dataType = inferType(wrsMetadata, i);
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
			String msg = "Problem reading in WebRowSet. " + e;
			logger.warn(msg);
			throw new SNEEDataSourceException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processWRSDataset(), number of tuples " + 
					tuples.size());
		return tuples;
	}

	private AttributeType inferType(ResultSetMetaData wrsMetadata, 
			int colIndex) 
	throws SQLException, TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER inferType() with column: " + colIndex);
		AttributeType dataType;
		int colType = wrsMetadata.getColumnType(colIndex);
		if (logger.isTraceEnabled())
			logger.trace("Column type code: " + colType);
		switch (colType) {
		case java.sql.Types.CHAR:
			dataType = _types.getType("string");
			break;
		case java.sql.Types.DECIMAL:
		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:
			dataType = _types.getType("float");
			break;
		case java.sql.Types.INTEGER:
			//Corresponds to INT or INTEGER
			dataType = _types.getType("integer");
			break;
		case java.sql.Types.TIMESTAMP:
			dataType = _types.getType("timestamp");
			break;
		default:
			String msg = "Unsupported data type " + 
				wrsMetadata.getColumnTypeName(colIndex);
			logger.warn(msg);
			throw new SchemaMetadataException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN inferType() with " + dataType);
		return dataType;
	}
    
}