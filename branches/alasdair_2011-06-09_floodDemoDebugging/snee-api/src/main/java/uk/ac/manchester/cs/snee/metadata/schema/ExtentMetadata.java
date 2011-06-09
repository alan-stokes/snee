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
package uk.ac.manchester.cs.snee.metadata.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;

public class ExtentMetadata {
	
	private Logger logger = 
		Logger.getLogger(ExtentMetadata.class.getName());

	/**
	 * List of the attributes in this extent
	 */
	private List<Attribute> _attributes =
		new ArrayList<Attribute>();

	private String _extentName;

	private ExtentType _extentType;

	//XXX-Ixent moved this to here from SensorNetworkSourceMetadata, as this seems 
	//to be a something considered during early steps of query optimization.
	private int cardinality = 1;
	
	private double rate;
	
	public ExtentMetadata(String extentName, 
			List<Attribute> attributes,
			ExtentType extentType) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER ExtentMetadata() with extent " + 
					extentName + " #attr=" + attributes.size() + 
					" type=" + extentType);
		_extentName = extentName;
		_attributes = attributes;
		_extentType = extentType;
		if (logger.isDebugEnabled())
			logger.debug("RETURN ExtentMetadata()");
	}

	public String getExtentName() {
		return _extentName;
	}

	/**
	 * Returns the type of the extent.
	 * @return
	 */
	public ExtentType getExtentType() {
		return _extentType;
	}

	/**
	 * Returns true if this extent is a stream.
	 * @return true if this extent is a stream
	 */
	public boolean isStream() {
		return _extentType.isStream();
	}

	/** 
	 * Returns the metadata about attributes
	 * @return The unqualified attribute names and their types
	 */
	public List<Attribute> getAttributes() {
		return _attributes;
	}

	/**
	 * Test if this extent has an attribute with the specified
	 * name.
	 * @param attribute name of the attribute to test for
	 * @return true if the extent has an attribute with the given name
	 */
	public boolean hasAttribute(String attribute) {
		boolean attrFound = false;
		for (Attribute attr : _attributes) {
			String attrName = attr.getAttributeSchemaName();
			if (attrName.equalsIgnoreCase(attribute)) {
				attrFound = true;
				break;
			}
		}
		return attrFound;
	}

	/**
	 * Returns the type of the attribute
	 * @param attribute
	 * @return the type of the attribute 
	 * @throws SchemaMetadataException if the specified attribute does not exist
	 */
	public AttributeType getAttributeType(String attribute)
	throws SchemaMetadataException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getAttributeType() with " + 
					attribute);
		}
		AttributeType type = null;
		for (Attribute attr : _attributes) {
			if (attribute.equalsIgnoreCase(
					attr.getAttributeSchemaName())) {
				type = attr.getType();
				break;
			}
		}
		if (type == null) {
			String message = "Extent " + _extentName + 
				" does not have an attribute " + attribute;
			logger.warn(message);
			throw new SchemaMetadataException(message);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getAttributeType() with " + 
					type);
		}
		return type;
	}
	
	/**
	 * Returns the number of attributes in the extent.
	 * 
	 * @return number of attributes
	 */
	public int getNumberAttribtues() {
		return _attributes.size();
	}

	/**
	 * Returns the physical size of the nesC representation of a tuple. 
	 * That is, the number of bytes used to represent the tuple.
	 * 
	 * @return number of bytes used to represent a tuple
	 */
	public int getNesCPhysicalSize() {
		int repSize = 0;
		for (Attribute attr : _attributes) {
			AttributeType type = attr.getType();
			repSize = repSize + type.getSize();
		}
		return repSize;
	}

	public boolean equals(Object ob) {
		boolean result = false;
		if (ob instanceof ExtentMetadata) {
			ExtentMetadata source = (ExtentMetadata) ob;
			//XXX: Not necessarily a complete check of source metadata equality
			/*
			 * Equality assumed if source refer to the same source type
			 * and the same extent.
			 */
			if (source.getExtentName().equalsIgnoreCase(_extentName) &&
					source.getExtentType() == _extentType)
					result = true;
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(_extentName);
		s.append(" - [");
		for (Attribute attr : _attributes) {
			s.append(attr);
			s.append("  ");
		}
		s.append("]");
		return s.toString();
	}

	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}

	public int getCardinality() {
		return cardinality;
	}

	public double getRate() throws MetadataException {
		if (_extentType == ExtentType.TABLE) {
			String message = "Rate not defined for " + ExtentType.TABLE; 
			logger.warn(message);
			throw new MetadataException(message);
		}
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}
	
}