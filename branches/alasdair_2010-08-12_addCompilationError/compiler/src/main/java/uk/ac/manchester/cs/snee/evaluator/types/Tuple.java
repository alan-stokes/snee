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

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;

/**
 * Tuple is the class that encapsulates a tuple. A tuple consists of a set
 * of fields, where each field has a unique name attribute
 */
public class Tuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4585575824334946852L;

//	Logger logger = Logger.getLogger(Tuple.class.getName());
	
	private Map<String,Field> _fields;

	DateFormat dateFormat = 
		new SimpleDateFormat(Constants.TIMESTAMP_FORMAT);

	public Tuple (){
		_fields = new HashMap<String,Field>();
	}
	
	public Tuple(Map<String,Field> fields) {
		_fields = fields;
	}

	public Map<String, Field> getFields(){
		return _fields;
	}

	public void addField(Field field) throws SNEEException {
		String fieldName = field.getName().toLowerCase();
		if (_fields.containsKey(fieldName)) {
//			logger.warn("Field " + fieldName + " already exists.");
			throw new SNEEException("Field " + fieldName + " already exists.");
		} else {
			_fields.put(fieldName, field);
		}
	}
	
	public void removeField(String fieldName) throws SNEEException {
		if (_fields.containsKey(fieldName)) {
			_fields.remove(fieldName);
		} else {
			throw new SNEEException("Unknown field name: " + fieldName);
		}
	}
	
	public Field getField(String fieldName) throws SNEEException{
		if (_fields.containsKey(fieldName.toLowerCase())) {
			return _fields.get(fieldName.toLowerCase());
		} else {
//			logger.warn("Unknown field name: " + fieldName);
			throw new SNEEException("Unknown field name: " + fieldName);
		}
	}
	
	public Object getValue(String fieldName) throws SNEEException {
		if (_fields.containsKey(fieldName.toLowerCase())) {
			return _fields.get(fieldName.toLowerCase()).getData();
		} else {
//			logger.warn("Unknown field name: " + fieldName);
			throw new SNEEException("Unknown field name: " + fieldName);
		}
	}

	public boolean containsField(String fieldName) {
		if (_fields.containsKey(fieldName.toLowerCase())) {
			return true;
		} else {
			return false;
		}
	}
	
	public String toString() {
		String retString="";
		Set<String> keys = _fields.keySet();
		for (String key : keys) {
			retString += key + ": " + _fields.get(key).toString() + ", ";
//		for (Field f : _fields.values()){
//			retString += f.toString() + ", ";
		}
		int lastCommaIndex = retString.lastIndexOf(",");
		if (lastCommaIndex > 0){
			retString = retString.substring(0, lastCommaIndex);
		}
		return retString;
	}

}
