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

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class TupleGenerator {

	Logger logger = Logger.getLogger(TupleGenerator.class.getName());
	private String _streamName;
	private List<Attribute> _columns;
	
	public TupleGenerator(ExtentMetadata stream) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER TupleGenerator() with stream " + 
					stream.getExtentName());
		}
		_streamName = stream.getExtentName();
		_columns = stream.getAttributes();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN TupleGenerator()");
		}
	}
	
	public Tuple generateTuple(int index) 
	throws SNEEException, TypeMappingException,
	SchemaMetadataException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER generateTuple() with index " + index);
		}
		Tuple tuple = new Tuple();
		for (Attribute attr : _columns) {
			EvaluatorAttribute field = 
				generateField(attr);
			tuple.addAttribute(field);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN generateTuple() with " + 
					_streamName + ":" + tuple);
		}
		return tuple;
	}
	
	private EvaluatorAttribute generateField(Attribute attr) 
	throws SchemaMetadataException, SNEEException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER generateField() with attr " + 
					attr);
		AttributeType attrType = attr.getType();
		Random random = new Random();
		Object value = null;
		switch (attr.getAttributeType()) {
		case Types.BOOLEAN:
			value = random.nextBoolean();
			break;
		case Types.DECIMAL:
			value = new BigDecimal(random.nextDouble());
			break;
		case Types.FLOAT:
			value = random.nextFloat();
			break;
		case Types.INTEGER:
			value = random.nextInt(10);
			break;
		case Types.CHAR:
		case Types.VARCHAR:
			value = _streamName;
			break;
		case Types.TIMESTAMP:
			value = System.currentTimeMillis();
			break;
		default:
			String message = "Unknown datatype " + attrType;
			logger.warn(message);
			throw new SNEEException(message);
		}
		EvaluatorAttribute evalAttr = 
			new EvaluatorAttribute(attr, value);
		if (logger.isTraceEnabled())
			logger.trace("RETURN generateField() with " + evalAttr);
		return evalAttr;
	}

	public String getStreamName() {
		return _streamName;
	}

}
