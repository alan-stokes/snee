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

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;

/**
 * Tuple is the class that represents a tuple. A tuple consists of 
 * a list of fields, where each field has a position and name attribute.
 * The attributes always appear in the same order, so can be accessed
 * by their attribute number.
 */
public class Tuple {

	private List<String> _attrNames = new ArrayList<String>();
	private List<String> _attrLabels = new ArrayList<String>();
	private List<String> _attrExtents = new ArrayList<String>();
	private List<EvaluatorAttribute> _attrValues = 
		new ArrayList<EvaluatorAttribute>();
	
	DateFormat dateFormat = 
		new SimpleDateFormat(Constants.TIMESTAMP_FORMAT);
	
	/**
	 * Create an empty tuple.
	 */
	public Tuple() {
	}

	/**
	 * Create a tuple with the supplied attribute values.
	 * 
	 * @param attrValues values for the attributes of the tuple
	 */
	public Tuple(List<EvaluatorAttribute> attrValues) {
		_attrValues = attrValues;
		/*
		 * Names, labels, and extents extracted to speed up
		 * access mechanisms
		 */
		for (EvaluatorAttribute attr : attrValues) {
			_attrNames.add(attr.getAttributeSchemaName());
			_attrLabels.add(attr.getAttributeDisplayName());
			_attrExtents.add(attr.getExtentName());
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
	 * Retrieve the display names of the attributes.
	 * 
	 * @return List of attribute display names
	 */
	public List<String> getAttributeDisplayNames() {
		return _attrLabels;
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
		String attrName = 
			attr.getAttributeSchemaName().toLowerCase();
		_attrNames.add(attrName);
		_attrLabels.add(attr.getAttributeDisplayName());
		_attrExtents.add(attr.getExtentName());
		_attrValues.add(attr);
	}
	
//	public void removeField(String fieldName) throws SNEEException {
//		if (_fields.containsKey(fieldName)) {
//			_fields.remove(fieldName);
//		} else {
//			throw new SNEEException("Unknown field name: " + fieldName);
//		}
//	}
	
	/**
	 * Retrieve the attribute at the given index.
	 * 
	 * @param index column number to retrieve
	 * @return data value for the column number
	 * @throws SNEEException index is out of range
	 */
	public EvaluatorAttribute getAttribute(int index) 
	throws SNEEException {
		EvaluatorAttribute attr;
		try {
			attr = _attrValues.get(index);
		} catch (IndexOutOfBoundsException e) {
			String message = "Index out of range for tuple size. ";
			throw new SNEEException(message);
		}
		return attr;
	}
	
	/**
	 * Retrieve the attribute of the first occurrence of the given 
	 * attribute name from the specified extent.
	 * 
	 * @param extentName name of the extent the attribute appears in
	 * @param attrName name of the attribute to retrieve
	 * @return field with the given field name
	 * @throws SNEEException field name does not exist
	 */
	public EvaluatorAttribute getAttribute(String extentName, 
			String attrName) 
	throws SNEEException {
		int index = findAttrIndex(extentName, attrName);
		return getAttribute(index);
	}
	
	/**
	 * Retrieve the attribute of the first occurrence of the given 
	 * display name.
	 * 
	 * @param attrDisplayName display name of the attribute to retrieve
	 * @return field with the given field name
	 * @throws SNEEException field name does not exist
	 */
	public EvaluatorAttribute getAttributeByDisplayName(
			String attrDisplayName) 
	throws SNEEException {
		int index = findAttrLabelIndex(attrDisplayName);
		return getAttribute(index);
	}
	
	/**
	 * Retrieve the data value of the field index specified.
	 * 
	 * @param index column number of the value to retrieve
	 * @return data value of the attribute
	 * @throws SNEEException index does not exist
	 */
	public Object getAttributeValue(int index) 
	throws SNEEException {
		Object data;
		try {
			data = _attrValues.get(index).getData();
		} catch (IndexOutOfBoundsException e) {
			String message = "Index out of range for tuple size. ";
			throw new SNEEException(message);
		}
		return data;
	}
	
	/**
	 * Retrieve the data value of the first occurrence of the given 
	 * attribute name from the specified extent.
	 * 
	 * @param extentName name of the extent the attribute appears in
	 * @param attrName name of the attribute to retrieve
	 * @return data value of the attribute
	 * @throws SNEEException field name does not exist
	 */
	public Object getAttributeValue(String extentName, 
			String attrName) 
	throws SNEEException {
		int index = findAttrIndex(extentName, attrName);
		return getAttributeValue(index);
	}
	
	/**
	 * Retrieve the data value stored in the first occurrence of the 
	 * display name.
	 * 
	 * @param attrDisplayName display name of the attribute to retrieve
	 * @return data value of the given attribute display name
	 * @throws SNEEException attribute display name does not exist
	 */
	public Object getAttributeValueByDisplayName(
			String attrDisplayName) 
	throws SNEEException {
		int index = findAttrLabelIndex(attrDisplayName);
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
	 * @param extentName name of the extent in which the attribute appears
	 * @param attrName name of the attribute
	 * @return index of the first occurrence of the corresponding attribute
	 * @throws SNEEException the field name does not exist
	 */
	private int findAttrIndex(String extentName, String attrName) 
	throws SNEEException {
		for (int i = 0; i < _attrNames.size(); i++) {
			String eName = _attrExtents.get(i);
			String aName = _attrNames.get(i);
//			System.out.println(extentName + " == " + eName);
//			System.out.println(attrName + " == " + aName);
			if (extentName.equalsIgnoreCase(eName) &&
					attrName.equalsIgnoreCase(aName)) {
				return i;
			}
		}
		throw new SNEEException("Unknown attribute name: " + 
				extentName + "." + attrName);
	}

	/**
	 * Returns the index of the first occurrence of the given 
	 * attribute label.
	 * 
	 * @param attrLabel display name of the attribute to find the index for
	 * @return index of the first occurrence of the corresponding attribute display name
	 * @throws SNEEException the field name does not exist
	 */
	private int findAttrLabelIndex(String attrLabel) 
	throws SNEEException {
		for (int i = 0; i < _attrLabels.size(); i++) {
			String name = _attrLabels.get(i);
			if (attrLabel.equalsIgnoreCase(name)) {
				return i;
			}
		}
		throw new SNEEException("Unknown attribute display name: " +
				attrLabel);
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < _attrNames.size(); i++) {
			buffer.append(_attrNames.get(i));
			buffer.append(": ");
			buffer.append(_attrValues.get(i).getData());
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
