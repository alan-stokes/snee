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

import java.util.ArrayList;
import java.util.List;

import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/** 
 * Wrapper for a String constant expression.
 */
public class StringLiteral implements Expression {

	/** Value of the constant. */
	private String value;
	private AttributeType _type;

	/**
	 * Constructor.
	 * @param newValue Constant value.
	 */
	public StringLiteral(String newValue, AttributeType type) {
		/* Parser returns constants still with single quotes */
		this.value = newValue.substring(1, newValue.length()-1);
		_type = type;
	}

	/** {@inheritDoc} */
	public String toString() {
		return (value);
	}

	/** {@inheritDoc} */
	public String getName() {
		return toString();
	}

	/** {@inheritDoc} */
	public List<Attribute> getRequiredAttributes() {
		return new ArrayList<Attribute>();
	}

	/** {@inheritDoc} 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException */
	public AttributeType getType() {
		return _type;
	}

	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return Empty list.
	 */
	public List<AggregationExpression> getAggregates()	{
		return new ArrayList<AggregationExpression>(0);
	}

	/**
	 * Finds the minimum value that this expression can return.
	 * @return The minimum value for this expressions
	 */
	public double getMinValue() {
		throw new AssertionError("Illegal call to getMinValue " +
				"on a StringLiteral");
	}

	/**
	 * Returns the value this <code>StringLiteral</code> represents.
	 * 
	 * @return String value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Finds the maximum value that this expression can return.
	 * @return The maximum value for this expressions
	 */
	public double getMaxValue() {
		throw new AssertionError("Illegal call to getMinValue " +
				"on a StringLiteral");
	}

	/**
	 * Finds the expected selectivity of this expression can return.
	 * @return The expected selectivity
	 * @throws AssertionError If Expression does not return a boolean.
	 */
	public double getSelectivity() {
		throw new AssertionError("Illegal call to getSelectivity");
	}

	/**
	 * Checks if the Expression can be directly used in an Aggregation Operator.
	 * Expressions such as attributes that can only be used inside a aggregation 
	 * expression return false.
	 * 
	 * @return true as it is possible to use constants. 
	 */
	public boolean allowedInAggregationOperator() {
		return true;
	}

	/**
	 * Checks if the Expression can be used in a Project Operator.
	 * 
	 * @return true  
	 */
	public boolean allowedInProjectOperator(){
		return true;
	}

	/**
	 * Converts this Expression to an Attribute.
	 * 
	 * @return The Attribute returned by this Expression.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public Attribute toAttribute() 
	throws SchemaMetadataException, TypeMappingException {
//		throw new SchemaMetadataException("Attempt to convert " +
//		"StringLiteral to a DataAttribute.");
		Attribute attribute = new DataAttribute("", this.getValue(), 
				this.getValue(), _type);
		return attribute;
	}

}
