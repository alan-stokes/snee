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
package uk.ac.manchester.cs.snee.operators.logical;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class ReceiveOperator extends InputOperator {

	/**
	 *  Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(ReceiveOperator.class.getName());
	
	/**
	 * Constructs a new Receive operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param source Metadata about data sources for the receive extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public ReceiveOperator(ExtentMetadata extentMetaData, 
			SourceMetadataAbstract source, 
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(extentMetaData, source, boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ReceiveOperator() with " +
					extentMetaData + " source=" + source.getSourceName());
					}
		this.setOperatorName("RECEIVE");
		this.setOperatorDataType(OperatorDataType.STREAM);	
		this.setOperatorSourceType(source.getSourceType());
		if (logger.isDebugEnabled())
			logger.debug("RETURN ReceiveOperator()");
	} 		 

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		logger.warn("Receive does not accept predicates");
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(
			List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushSelectDown(Expression predicate) {
		return false;
	}

}
