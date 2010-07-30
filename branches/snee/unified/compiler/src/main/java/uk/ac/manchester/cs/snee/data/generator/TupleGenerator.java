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
package uk.ac.manchester.cs.snee.data.generator;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class TupleGenerator {

	Logger logger = Logger.getLogger(TupleGenerator.class.getName());
	private String _streamName;
	private Map<String, AttributeType> _columns;
	private Types _types;
	
	public TupleGenerator(ExtentMetadata stream, Types types) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER TupleGenerator() with stream " + 
					stream.getExtentName());
		}
		_types = types;
		_streamName = stream.getExtentName();
		_columns = stream.getAttributes();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN TupleGenerator()");
		}
	}
	
	public Tuple generateTuple(int index) 
	throws SNEEException, TypeMappingException, SchemaMetadataException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER generateTuple() with index " + index);
		}
		Tuple tuple = new Tuple();
		for (String attrName : _columns.keySet()) {
			AttributeType attrType = _columns.get(attrName);
			try {
				Field field = generateField(attrName, attrType);
				tuple.addField(field);
			} catch (SNEEException e) {
				if (logger.isEnabledFor(Level.WARN)) {
					logger.warn("Unknown data type \"" + 
							attrType + "\". Ignored.");
				}
				throw e;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN generateTuple() with " + _streamName + 
					":" + tuple);
		}
		return tuple;
	}
	
	private Field generateField(String attrName, AttributeType attrType) 
	throws SNEEException, TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER generateField() with attrName " + 
					attrName + " attrType " + attrType);
		Random random = new Random();
		Object value = null;
		if (attrType.getName().equals("integer")) {
			value = random.nextInt(10);
		} else if (attrType.getName().equals("float")) {
			value = random.nextFloat();
		} else if (attrType.getName().equals("string")) {
			value = Long.toString(Math.abs(random.nextLong()), 36);
		} else if (attrType.getName().equals("boolean")) {
			value = random.nextBoolean();
		} else if (attrType.getName().equals("timestamp")) {
			value = System.currentTimeMillis();
//		} else if (attrType == AttributeType.DATETIME_TYPE) {
//			value = new Date();
//		} else if (attrType == AttributeType.TIME_TYPE) {
//			Calendar cal = Calendar.getInstance();
//			value = cal.getTime();
		} else {
			String message = "Unknown datatype " + attrType;
			logger.warn(message);
			throw new SNEEException(message);
		}
		Field field = new Field(attrName, attrType, value);
		if (logger.isTraceEnabled())
			logger.trace("RETURN generateField() with " + field);
		return field;
	}

	public String getStreamName() {
		return _streamName;
	}

}
