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

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;

/**
 * Represents a field of a tuple.
 * 
 * A field has a name, a data type, a value, 
 * and an index representing its position in the tuple
 *
 */
public class Field implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6878277846186499888L;
	
	DateFormat timestampFormat = 
		new SimpleDateFormat(Constants.TIMESTAMP_FORMAT);

	/**
	 * Data type of the field
	 */
	private AttributeType _dataType;
	
	/**
	 * The name of the field
	 */
	private String _name;
	
	/**
	 * The value of the field 
	 */
	private Object _data;
	
	/**
	 * Creates a representation of a field within a tuple
	 * @param name field name
	 * @param dataType field data type
	 * @param value value stored by the field
	 */
	public Field(String name, AttributeType dataType, Object value) {
		_name = name;
		_dataType = dataType;
		//XXX: What if someone passes me an object of the wrong type?
		_data = value;
	}
	
	public String getName() {
		return _name;
	}
	
	public AttributeType getDataType() {
		return _dataType;
	}
	
	public Object getData() {
		return _data;
	}
	
	public String toString() {
//		if (_dataType.equals(AttributeType.TIME)) {
//			return new SimpleDateFormat("HH:mm:ss.SSS Z").format(_data);
//		} else if (_dataType.equals(AttributeType.DATE)) {
//			return new SimpleDateFormat("yyyy-mm-dd").format(_data);
//		} else if (_dataType.equals(AttributeType.DATETIME)) {
//			return timestampFormat.format(_data);
//		} else {
			return _data.toString();
//		}
	}

	public void setName(String newName) {
		_name = newName;
	}

}
