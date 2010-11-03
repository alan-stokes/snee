package uk.ac.manchester.cs.snee.datasource.webservice;

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

	protected AttributeType inferType(int sqlType) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER inferType() for " + sqlType); 
		AttributeType type;
		if (logger.isTraceEnabled())
			logger.trace("SqlType: " + sqlType);
		switch (sqlType) {
		case java.sql.Types.BOOLEAN:
			type = _types.getType("boolean");
			break;
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			type = _types.getType("string");
			break;
		case java.sql.Types.DECIMAL:
			type = _types.getType("decimal");
			break;
		case java.sql.Types.FLOAT:
			type = _types.getType("float");
			break;
		case java.sql.Types.INTEGER:
		case java.sql.Types.BIGINT:
			type = _types.getType("integer");			
			break;
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.DATE:
			//FIXME: Do we really want date to be a timestamp?
			type = _types.getType("timestamp");
			break;
		default:
			if (sqlType == 7) {
				logger.trace("Hack to overcome misreported data " +
						"type in CCO-WS metadata. " +
						"Real being mapped to float.");
				type = _types.getType("float");
			} else {
				String msg = "Unsupported attribute type " + sqlType;
				logger.warn(msg);
				throw new SchemaMetadataException(msg);
//				type = _types.getType("string");
			}
			break;
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN inferType() with " + type);
		return type;
	}
	
}
