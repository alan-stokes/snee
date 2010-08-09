package uk.ac.manchester.cs.snee.data.webservice;

import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;

public abstract class SchemaParserAbstract implements SchemaParser {

	private Logger logger = 
		Logger.getLogger(SchemaParserAbstract.class.getName());
	
	private Types _types;
	
	protected SchemaParserAbstract(Types types) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SchemaParserAbstract()");
		_types = types;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SchemaParserAbstract()");
		}
	}

	protected AttributeType inferType(String sqlType) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER inferType() for " + sqlType); 
		AttributeType type;
		if (logger.isTraceEnabled())
			logger.trace("SqlType: " + sqlType);
		if (sqlType.equalsIgnoreCase("INT") || 
				sqlType.equalsIgnoreCase("INTEGER")) {
			type = _types.getType("integer");
		} else if (sqlType.equalsIgnoreCase("CHAR")) {
			type = _types.getType("string");
		} else if (sqlType.equalsIgnoreCase("DECIMAL") ||
				sqlType.equalsIgnoreCase("FLOAT")) {
			type = _types.getType("float");
		} else if (sqlType.equalsIgnoreCase("DATETIME")) {
			type = _types.getType("string");
		} else {
			String msg = "Unsupported attribute type " + sqlType;
			logger.warn(msg);
			throw new SchemaMetadataException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN inferType() with " + type);
		return type;
	}
	
}
