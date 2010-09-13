/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
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
package uk.ac.manchester.cs.snee.evaluator.types;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;

//TODO: Link with Attribute class
/**
 * Tuple is the class that represents a tuple. A tuple consists of 
 * a list of fields, where each field has a position and name attribute.
 * The attributes always appear in the same order, so can be accessed
 * by their attribute number.
 */
public class Tuple {

	private static Logger logger = 
		Logger.getLogger(Tuple.class.getName());

	private List<String> _attrNames = new ArrayList<String>();
	private List<EvaluatorAttribute> _attrValues = new ArrayList<EvaluatorAttribute>();
	
	DateFormat dateFormat = 
		new SimpleDateFormat(Constants.TIMESTAMP_FORMAT);
	
	/**
	 * Create an empty tuple.
	 */
	public Tuple() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER Tuple()");
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN Tuple()");
		}
	}

	/**
	 * Create a tuple with the supplied attribute values.
	 * 
	 * @param attrValues values for the attributes of the tuple
	 */
	public Tuple(List<EvaluatorAttribute> attrValues) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER Tuple() with #attrs=" + 
					attrValues.size());
		}
		_attrValues = attrValues;
		for (EvaluatorAttribute attr : attrValues) {
			_attrNames.add(attr.getAttributeSchemaName());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN Tuple()");
		}
	}

	/**
	 * Retrieve the names of the attributes.
	 * 
	 * @return List of attribute names
	 */
	public List<String> getAttributeNames() {
		return _attrNames;
	}
	
	/**
	 * Retrieve the <code>EvaluatorAttribute</code> objects
	 * representing the attributes of the tuple.
	 * 
	 * @return List of <code>EvaluatorAttribute</code> objects
	 */
	public List<EvaluatorAttribute> getAttributeValues() {
		return _attrValues;
	}

	/**
	 * Add the supplied <code>EvaluatorAttribute</code> to the
	 * tuple.
	 * 
	 * @param attr attribute to be added
	 */
	public void addAttribute(EvaluatorAttribute attr) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER addField() with " + attr);
		}
		String attrName = 
			attr.getAttributeSchemaName().toLowerCase();
		_attrNames.add(attrName);
		_attrValues.add(attr);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN addField()");
		}
	}
	
//	public void removeField(String fieldName) throws SNEEException {
//		if (_fields.containsKey(fieldName)) {
//			_fields.remove(fieldName);
//		} else {
//			throw new SNEEException("Unknown field name: " + fieldName);
//		}
//	}
	
	/**
	 * Retrieve the field at the given index.
	 * 
	 * @param index column number to retrieve
	 * @return field value for the column number
	 * @throws SNEEException index is out of range
	 */
	public EvaluatorAttribute getAttribute(int index) 
	throws SNEEException {
		EvaluatorAttribute field;
		try {
			field = _attrValues.get(index);
		} catch (IndexOutOfBoundsException e) {
			String message = "Index out of range for tuple size. ";
			logger.warn(message, e);
			throw new SNEEException(message);
		}
		return field;
	}
	
	/**
	 * Retrieve the field of the first occurrence of the given 
	 * field name.
	 * 
	 * @param fieldName name of the field to retrieve
	 * @return field with the given field name
	 * @throws SNEEException field name does not exist
	 */
	public EvaluatorAttribute getAttribute(String fieldName) 
	throws SNEEException {
		int index = findFieldIndex(fieldName);
		return getAttribute(index);
	}
	
	/**
	 * Retrieve the data value of the field index specified.
	 * 
	 * @param index column number of the value to retrieve
	 * @return object representing the field value
	 * @throws SNEEException index does not exist
	 */
	public Object getAttributeValue(int index) 
	throws SNEEException {
		Object data;
		try {
			data = _attrValues.get(index).getData();
		} catch (IndexOutOfBoundsException e) {
			String message = "Index out of range for tuple size. ";
			logger.warn(message, e);
			throw new SNEEException(message);
		}
		return data;
	}
	
	/**
	 * Retrieve the value stored in the first occurrence of the field 
	 * with the given name.
	 * 
	 * @param fieldName name of the field to retrieve
	 * @return value of the given field name
	 * @throws SNEEException field name does not exist
	 */
	public Object getAttributeValue(String fieldName) 
	throws SNEEException {
		int index = findFieldIndex(fieldName);
		return getAttributeValue(index);
	}
	
	/**
	 * Returns the number of attributes in the tuple.
	 * 
	 * @return number of attributes
	 */
	public int size() {
		return _attrValues.size();
	}

	/**
	 * Returns the index of the first occurrence of the given 
	 * field name.
	 * 
	 * @param fieldName name of the field to find the index for
	 * @return index of the first occurrence of the corresponding field
	 * @throws SNEEException the field name does not exist
	 */
	private int findFieldIndex(String fieldName) 
	throws SNEEException {
		for (int i = 0; i < _attrNames.size(); i++) {
			String name = _attrNames.get(i);
			if (fieldName.equalsIgnoreCase(name)) {
				return i;
			}
		}
		throw new SNEEException("Unknown field name: " + fieldName);
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < _attrNames.size(); i++) {
			buffer.append(_attrNames.get(i));
			buffer.append(": ");
			buffer.append(_attrValues.get(i));
			buffer.append(", ");
		}
		int lastCommaIndex = buffer.lastIndexOf(",");
		String retString;
		if (lastCommaIndex > 0){
			retString = buffer.substring(0, lastCommaIndex);
		} else {
			retString = buffer.toString();
		}
		return retString;
	}

}
