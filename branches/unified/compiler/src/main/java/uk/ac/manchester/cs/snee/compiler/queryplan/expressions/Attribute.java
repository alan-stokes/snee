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

import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;

/**
 * 
 * @author Christian
 *
 */
public abstract class Attribute implements Expression {

	/** Holds the localName for the extent. */
	private String localName;

	/** Hold the name of the attribute. */
	private String attributeName;
	
	/** Constructor for subclasses. */
	protected Attribute() {
	}
	
	/** Constructor for sub classes.
	 * @param newLocalName new value for localName (extent name)
	 * @param newAttributeName new value for the attribute name.
	 */
	protected Attribute(String newLocalName, String newAttributeName) {
		this.localName = newLocalName;
		this.attributeName = newAttributeName;
	}
	
	/** 
	 * List of the attributes required to produce this expression.
	 * 
	 * @return The zero or more attributes required for this expression.
	 * Return may contain duplicates.
	 */
	public ArrayList<Attribute> getRequiredAttributes() {
		ArrayList<Attribute> result = new ArrayList<Attribute>(1);
		result.add(this);
		return result;
	}
	
	/**
	 * Accessor method.
	 * @return The Attribute Name.
	 */
	public String getAttributeName() {
		return attributeName;
	}
	/**
	 * Accessor method.
	 * @return The local (or extent) name.
	 */
	public String getLocalName() {
		return localName;
	}
	
	/** 
	 * Extracts the aggregates from within this expression.
	 * 
	 * @return Empty list.
	 */
	public ArrayList<AggregationExpression> getAggregates()	{
		return new ArrayList<AggregationExpression>(0);
	}

	/**
	 * Sets the local name.
	 * @param newLocalName new value.
	 */
	public void setLocalName(String newLocalName) {
		this.localName = newLocalName;
	}

	/**
	 * Sets the attribute name.
	 * @param newAttributeName new value.
	 */
	protected void setAttributeName(String newAttributeName) {
		this.attributeName = newAttributeName;
	}
}
