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
package uk.ac.manchester.cs.snee.compiler.queryplan.expressions;

import java.util.List;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

/**
 * Interface for any expression.
 * Includs arithmatic, aggeragation and boolean expressions.
 * @author Christian
 */
public interface Expression {

	/** 
	 * List of the attributes required to produce this expression.
	 * 
	 * @return The zero or more attributes required for this expression.
	 * Return may contain duplicates.
	 */
	List<Attribute> getRequiredAttributes();
	
	/**
	 * The raw data type of this expression.
	 *  
	 * @return The raw data type of this expression.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	AttributeType getType() 
	throws SchemaMetadataException, TypeMappingException;
	
	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return A List of all the aggregates within this expressions.
	 * Could contain duplicates.
	 */
	List<AggregationExpression> getAggregates();
	
	/**
	 * Finds the minimum value that this expression can return.
	 * @return The minimum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMinValue();
	
	/**
	 * Finds the maximum value that this expression can return.
	 * @return The maximum value for this expressions
	 * @throws AssertionError If Expression returns a boolean.
	 */
	public double getMaxValue();
	
	/**
	 * Finds the expected selectivity of this expression can return.
	 * @return The expected selectivity
	 * @throws AssertionError If Expression does not return a boolean.
	 */
	public double getSelectivity();
	
	/**
	 * Checks if the Expression can be directly used in an Aggregation Operator.
	 * Expressions such as attributes that can only be used inside a aggregation expression return false.
	 * 
	 * @return true if this expression can be directly used in a Aggregation Operator. 
	 * @throws ParserValidationException 
	 */
	public boolean allowedInAggregationOperator() 
	throws ExpressionException;
	
	/**
	 * Checks if the Expression can be used in a Project Operator.
	 * 
	 * @return true if this expression can be used in a Project Operator. 
	 */
	public boolean allowedInProjectOperator();

	/**
	 * Converts this Expression to an Attribute.
	 * 
	 * @return The Attribute returned by this Expression.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public Attribute toAttribute() 
	throws SchemaMetadataException, TypeMappingException;
	
}	
