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

import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;

/**
 * Temporary attribute used for incremental aggregations, e.g., count/sum when computing an average.
 * @author ixent
 *
 */
public class IncrementalAggregationAttribute extends DataAttribute {

	/**
	 * The attribute that this one is derived from.
	 */
	private DataAttribute baseAttribute;
	
	/**
	 * The type of aggregation function represented by this attribute.
	 */
	private AggregationFunction aggrFunction;
	
	/**
	 * Construct a IncrementalAggregationAttribute instance
	 * 
	 * @param extentName name of the extent the attribute appears in
	 * @param attrName name of the attribute as it appears in the schema
	 * @param attrType type of the attribute
	 * @throws SchemaMetadataException
	 */
	public IncrementalAggregationAttribute(String extentName, String attrName,
			AttributeType attrType, DataAttribute baseAttribute, AggregationFunction aggrFunction) 
	throws SchemaMetadataException {
		super(extentName, attrName, attrType);
		this.baseAttribute = baseAttribute;
		this.aggrFunction = aggrFunction;
	}

	/**
	 * Construct a IncrementalAggregationAttribute instance
	 * 
	 * @param extentName name of the extent the attribute appears in
	 * @param attrName name of the attribute as it appears in the schema
	 * @param attrLabel display label for the attribute
	 * @param attrType type of the attribute
	 * @throws SchemaMetadataException
	 */
	public IncrementalAggregationAttribute(String extentName, String attrName,
			String attrLabel, AttributeType attrType, DataAttribute baseAttribute, AggregationFunction aggrFunction) 
	throws SchemaMetadataException {
		super(extentName, attrName, attrLabel, attrType);
		this.baseAttribute = baseAttribute;
		this.aggrFunction = aggrFunction;
	}

	public IncrementalAggregationAttribute(Attribute attr, DataAttribute baseAttribute, AggregationFunction aggrFunction) 
	throws SchemaMetadataException {
		super(attr);
	}

	public DataAttribute getBaseAttribute() {
		return baseAttribute;
	}

	public AggregationFunction getAggrFunction() {
		return aggrFunction;
	}

}
