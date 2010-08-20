package uk.ac.manchester.cs.snee.compiler.metadata.schema;


public class Attribute {

	private String _name;
	private String _extentName;
	private String _label = null;
	private AttributeType _type;
	private SQLTypes _sqlType;
	
	public Attribute(String extentName, String name, AttributeType type) 
	throws SchemaMetadataException {
		_extentName = extentName;
		_name = name;
		_type = type;
		inferSQLType(type);
	}

	private void inferSQLType(AttributeType type)
	throws SchemaMetadataException {
		String typeName = type.getName();
		if (typeName.equalsIgnoreCase("boolean")) {
			_sqlType = SQLTypes.BOOLEAN;
		} else if (typeName.equalsIgnoreCase("float")) {
			_sqlType = SQLTypes.FLOAT;
		} else if (typeName.equalsIgnoreCase("integer")) {
			_sqlType = SQLTypes.INTEGER;
		} else if (typeName.equalsIgnoreCase("string")) {
			_sqlType = SQLTypes.VARCHAR;
		} else if (typeName.equalsIgnoreCase("timestamp")) {
			_sqlType = SQLTypes.TIMESTAMP;
		} else {
			throw new SchemaMetadataException("Unsupported data type " +
					typeName);
		}
	}

	/**
	 * Retrieves the name of the attribute.
	 * 
	 * @return the attribute name as it appears in the schema
	 */
	public String getName() {
		return _name;
	}
	
	/**
	 * Retrieves the name of the extent in which this attribute appears.
	 * 
	 * @return the extent name
	 */
	public String getExtentName() {
		return _extentName;
	}

	/**
	 * Retrieves the name label that has been associated with this
	 * attribute by a query.
	 * 
	 * @return the label for the attribute; 
	 * <code>extentName.attributeName</code> returned if not set
	 * @see Attribute#setAttributeLabel(String)
	 */
	public String getAttributeLabel() {
		String result;
		if (_label == null) {
			result = _extentName + "." + _name;
		} else {
			result = _label;
		}
		return result;
	}
	
	/**
	 * Associates a label with this attribute in the query.
	 * 
	 * @param label the name to be associated with this attribute
	 * @see Attribute#getAttributeLabel()
	 */
	public void setAttributeLabel(String label) {
		_label = label;
	}
	
	public AttributeType getType() {
		return _type;
	}
	
	/**
	 * Retrieves the type code (one of the 
	 * <code>java.sql.Types</code> constants) for the SQL type of the 
	 * value stored in the designated attribute.
	 * 
	 * @return an int representing the SQL type of the attribute
	 * @see Attribute#getAttributeTypeName()
	 */
	public int getAttributeType() {
		return _sqlType.getSQLType();
	}
	
	/**
	 * Retrieves the SNEE type name for values stored in the attribute.
	 * 
	 * @return the type name used by SNEE
	 * @see Attribute#getAttributeType()
	 */
	public String getAttributeTypeName() {
		return _sqlType.toString();
	}
	
	@Override
	public boolean equals(Object ob) {
		boolean result = false;
		if (ob instanceof Attribute) {
			Attribute attr = (Attribute) ob;
			if (attr.getExtentName().equalsIgnoreCase(_extentName) &&
					attr.getName().equalsIgnoreCase(_name) &&
					attr.getType() == _type) {				
				result = true;
			}
		}
		return result;
	}
	
	public String toString() {
		return _extentName + "." + _name + ":" + _type.getName() + 
			"(" + _sqlType + ")";
	}
}
