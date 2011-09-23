package uk.ac.manchester.cs.snee.datasource.webservice;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;

public abstract class SchemaParserAbstract implements SchemaParser {

	private static final Logger logger = Logger.getLogger(SchemaParserAbstract.class.getName());

	private SQLUtils sqlUtils;
	
	protected SchemaParserAbstract(Types types) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SchemaParserAbstract()");
		sqlUtils = new SQLUtils(types);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SchemaParserAbstract()");
		}
	}

	protected AttributeType inferType(int sqlType) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER inferType() for " + sqlType); 
		AttributeType type;
		type = sqlUtils.inferType(sqlType);
		if (logger.isTraceEnabled())
			logger.trace("RETURN inferType() with " + type);
		return type;
	}
	
}
