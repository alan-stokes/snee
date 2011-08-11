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

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

/**
 * Represents an attribute of a tuple used by the evaluator.
 * 
 * An EvaluatorAttribute extends the logical 
 * <code>Attribute</code> with a data value.
 *
 * @see Attribute
 */
public class EvaluatorAttribute extends DataAttribute {
	
	/**
	 * The value of the field 
	 */
	private Object _data;

	/**
	 * Constructor for EvaluatorAttribute implementation which 
	 * extends the logical Attribute class with a data object.
	 * 
	 * @param extentName name of the extent from which the attribute is derived
	 * @param attrName attribute name as it appears in the schema
	 * @param attrType type of the attribute
	 * @throws SchemaMetadataException invalid data type
	 */
	public EvaluatorAttribute(String extentName, String attrName,
			AttributeType attrType, Object data) 
	throws SchemaMetadataException {
		super(extentName, attrName, attrType);
		_data = data;
	}
	
	/**
	 * Constructor for EvaluatorAttribute implementation which 
	 * extends the logical Attribute class with a data object.
	 * 
	 * @param extentName name of the extent from which the attribute is derived
	 * @param attrName attribute name as it appears in the schema
	 * @param attrLabel display name of the attribute
	 * @param attrType type of the attribute
	 * @throws SchemaMetadataException invalid data type
	 */
	public EvaluatorAttribute(String extentName, String attrName, 
			String attrLabel, AttributeType attrType, Object data)
	throws SchemaMetadataException {
		super(extentName, attrName, attrLabel, attrType);
		_data = data;
	}

	/**
	 * Constructor for EvaluatorAttribute implementation which 
	 * extends the logical Attribute class with a data object.
	 * 
	 * @param attr <code>Attribute</code> that is being extended
	 * @param data data value being stored
	 * @throws SchemaMetadataException invalid data type
	 */
	public EvaluatorAttribute(Attribute attr, Object data) 
	throws SchemaMetadataException {
		super(attr);
		_data = data;
	}
	
	public Object getData() {
		return _data;
	}
	
	public String toString() {
		String valueString;
		if (_data == null) {
			valueString = null;
		} else {
			valueString = _data.toString();
		}
		return super.toString() + " " + valueString;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_data == null) ? 0 : _data.hashCode());
		return result;
	}	

}
