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
package uk.ac.manchester.cs.snee.compiler.metadata.schema;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ExtentMetadata {
	
	private Logger logger = 
		Logger.getLogger(ExtentMetadata.class.getName());

	/*
	 * Attribute name (String key) and type. 
	 */
	private Map<String, AttributeType> _attributes = 
		new LinkedHashMap<String, AttributeType>();

	private String _extentName;

	private ExtentType _extentType;

	//Ixent moved this to here from SensorNetworkSourceMetadata, as this seems 
	//to be a something considered during early steps of query optimization.
	private int cardinality = 1;
	
	public ExtentMetadata(String extentName, 
			Map<String, AttributeType> attributes,
			ExtentType extentType) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER ExtentMetadata() with extent " + 
					extentName + " #attr= " + attributes.size() + 
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
	public Map<String, AttributeType> getAttributes() {
		return _attributes;
	}

	public boolean hasAttribute(String attribute) {
		return _attributes.containsKey(attribute);
	}

	/**
	 * Returns the type of the attribute
	 * @param attribute
	 * @return the type of the attribute 
	 * @throws SchemaMetadataException if the specified attribute does not exist
	 */
	public AttributeType getAttributeType(String attribute)
	throws SchemaMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getAttributeType() with " + attribute);
		AttributeType type;
		if (_attributes.containsKey(attribute)) {
			type = _attributes.get(attribute);
		} else {
			String message = "Extent " + _extentName + 
				" does not have an attribute " + attribute;
			logger.warn(message);
			throw new SchemaMetadataException(message);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getAttributeType() with " + type);
		return type;
	}

	/**
	 * Returns the size of the tuple.
	 * @return
	 */
	public int getSize() {
		int count = 0;
		Iterator<AttributeType> types = _attributes.values().iterator();
		while (types.hasNext()) {
			count = count + types.next().getSize();
		}
		return count;
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
		Iterator<String> it = _attributes.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			s.append(key);
			s.append(":");
			s.append(_attributes.get(key));
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
	
}