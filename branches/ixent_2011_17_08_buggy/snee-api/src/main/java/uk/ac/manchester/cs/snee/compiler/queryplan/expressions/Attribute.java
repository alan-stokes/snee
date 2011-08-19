/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.compiler.queryplan.expressions;

import java.util.ArrayList;
import java.util.List;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SQLTypes;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;

public abstract class Attribute implements Expression {

	/** 
	 * Holds the attribute name as it appears in the schema.
	 */
	protected String attributeSchemaName;

	/** 
	 * Hold the display name of the attribute.
	 * This can be changed in the query using the project
	 * rename operation.
	 */
	private String attributeLabel;
	
	/** 
	 * Holds the name of the extent in which this attribute 
	 * occurs in the schema.
	 */
	protected String extentName;
	
	/**
	 * Holds the type of this attribute as declared in the
	 * schema.
	 */
	protected AttributeType type;
	
	/**
	 * Holds the corresponding SQL type for the attribute
	 * type declared in the schema.
	 */
	protected SQLTypes sqlType;
	
	/**
	 * Holds the status of this attribute as to whether it is representing
	 * a constant value or not. Default is false. 
	 */
	protected boolean isConstant = false;

	private List<Attribute> requiredAttributes = new ArrayList<Attribute>(1);
	
	/** 
	 * @param extentName name of the extent in which this attribute appears
	 * @param attrName attribute name as it appears in the schema
	 * @param type the declared type of the attribute
	 * @throws SchemaMetadataException the declared data type does not have a corresponding SQLType
	 */
	public Attribute(String extentName, String attrName, 
			AttributeType type) 
	throws SchemaMetadataException {
		this.extentName = extentName;
		this.attributeSchemaName = attrName;
		this.attributeLabel = extentName + "." + attrName;
		this.type = type;
		inferSQLType(type);
		requiredAttributes.add(this);
	}
	
	/** 
	 * @param extentName name of the extent in which this attribute appears
	 * @param attrName attribute name as it appears in the schema
	 * @param attrLabel display value for the attribute name.
	 * @param type the declared type of the attribute
	 * @throws SchemaMetadataException the declared data type does not have a corresponding SQLType
	 */
	public Attribute(String extentName, String attrName, 
			String attrLabel, AttributeType type) 
	throws SchemaMetadataException {
		this.extentName = extentName;
		this.attributeSchemaName = attrName;
		if (attrLabel == null) {
			this.attributeLabel = extentName + "." + attrName;
		} else {
			this.attributeLabel = attrLabel;
		}
		this.type = type;
		inferSQLType(type);
		requiredAttributes.add(this);
	}

	public Attribute(Attribute attr) throws SchemaMetadataException {
		this.attributeSchemaName = attr.getAttributeSchemaName();
		this.attributeLabel = attr.getAttributeDisplayName();
		this.extentName = attr.getExtentName();
		this.type = attr.getType();
		inferSQLType(type);
		requiredAttributes.addAll(attr.getRequiredAttributes());
	}

	private void inferSQLType(AttributeType type)
	throws SchemaMetadataException {
		String typeName = type.getName();
		if (typeName.equalsIgnoreCase("boolean")) {
			sqlType = SQLTypes.BOOLEAN;
		} else if (typeName.equalsIgnoreCase("decimal")) {
			sqlType = SQLTypes.DECIMAL;
		} else if (typeName.equalsIgnoreCase("float")) {
			sqlType = SQLTypes.FLOAT;
		} else if (typeName.equalsIgnoreCase("integer")) {
			sqlType = SQLTypes.INTEGER;
		} else if (typeName.equalsIgnoreCase("string")) {
			sqlType = SQLTypes.VARCHAR;
		} else if (typeName.equalsIgnoreCase("timestamp")) {
			sqlType = SQLTypes.TIMESTAMP;
		} else {
			throw new SchemaMetadataException("Unsupported data type " +
					typeName);
		}
	}
	
	/** 
	 * List of the attributes required to produce this expression.
	 * 
	 * @return The zero or more attributes required for this expression.
	 * Return may contain duplicates.
	 */
	public List<Attribute> getRequiredAttributes() {
		return requiredAttributes;
	}
	
	public void setRequiredAttributes(List<Attribute> requiredAttributes) {
		this.requiredAttributes = requiredAttributes;
	}
	
	/**
	 * Return the label for the attribute as declared in the
	 * query.
	 * @return The attribute label; 
	 * <code>extentName.attributeName</code> returned if not set
	 * @see Attribute#setAttributeDisplayName(String)
	 */
	public String getAttributeDisplayName() {
		String result;
		if (attributeLabel == null) {
			result = extentName + "." + attributeSchemaName;
		} else {
			result = attributeLabel;
		}
		return result.toLowerCase();
	}
	
	/**
	 * Associates a label with this attribute in the query.
	 * 
	 * @param label the name to be associated with this attribute
	 * @see Attribute#getAttributeLabel()
	 */
	public void setAttributeDisplayName(String label) {
		attributeLabel = label;
	}

	/**
	 * Return the name for the attribute as declared in the schema
	 * @return The local (or extent) name.
	 */
	public String getAttributeSchemaName() {
		return attributeSchemaName.toLowerCase();
	}

	/**
	 * Retrieves the name of the extent in which this attribute appears.
	 * 
	 * @return the extent name
	 */
	public String getExtentName() {
		return extentName.toLowerCase();
	}
	
	public AttributeType getType() {
		return type;
	}
	
	/**
	 * Retrieves the type code (one of the 
	 * <code>java.sql.Types</code> constants) for the SQL type 
	 * of the value stored in the designated attribute.
	 * 
	 * @return an int representing the SQL type of the attribute
	 * @see Attribute#getAttributeTypeName()
	 */
	public int getAttributeType() {
		return sqlType.getSQLType();
	}
	
	/**
	 * Retrieves the SNEE type name for values stored in the 
	 * attribute.
	 * 
	 * @return the type name used by SNEE
	 * @see Attribute#getAttributeType()
	 */
	public String getAttributeTypeName() {
		return sqlType.toString().toLowerCase();
	}
	
	@Override
	public boolean equals(Object ob) {
		boolean result = false;
		if (ob instanceof Attribute) {
			Attribute attr = (Attribute) ob;
			if (attr.getExtentName().equalsIgnoreCase(extentName) &&
					attr.getAttributeSchemaName().equalsIgnoreCase(attributeSchemaName) &&
					attr.getType() == type) {				
				result = true;
			}
		}
		return result;
	}
	
	public String toString() {
		return extentName + "." + attributeSchemaName + ":" +
			type.getName() + "(" + sqlType + ")";
	}	
	
	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return Empty list.
	 */
	public List<AggregationExpression> getAggregates()	{
		return new ArrayList<AggregationExpression>(0);
	}

	public void setIsConstant(boolean isConstant) {
		this.isConstant = isConstant;
	}

	public boolean isConstant() {
		return isConstant;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression#setIsJoinCondition(boolean)
	 */
	public void setIsJoinCondition(boolean isJoin) {
		throw new AssertionError("An attribute cannot be a join condition.");
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression#isJoinCondition()
	 */
	public boolean isJoinCondition() {
		return false;
	}
	
}
