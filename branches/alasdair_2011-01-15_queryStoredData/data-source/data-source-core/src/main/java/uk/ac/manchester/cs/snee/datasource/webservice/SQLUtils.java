package uk.ac.manchester.cs.snee.datasource.webservice;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;

public class SQLUtils {

	private Logger logger = 
		Logger.getLogger(SQLUtils.class.getName());
	
	private Types _types;
	
	public SQLUtils(Types types) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SQLUtils()");
		_types = types;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SQLUtils()");
		}
	}

	public AttributeType inferType(int sqlType) 
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
		case java.sql.Types.LONGVARCHAR:
			type = _types.getType("string");
			break;
		case java.sql.Types.DECIMAL:
			type = _types.getType("decimal");
			break;
		case java.sql.Types.FLOAT:
		case java.sql.Types.REAL:
			type = _types.getType("float");
			break;
		case java.sql.Types.INTEGER:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.BIGINT:
			type = _types.getType("integer");			
			break;
		case java.sql.Types.BIT:
			//FIXME: Really should introduce the bit type again!
			type = _types.getType("integer");
			break;
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.TIME:
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
