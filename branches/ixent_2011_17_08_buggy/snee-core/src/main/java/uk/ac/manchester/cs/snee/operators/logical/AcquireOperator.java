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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class AcquireOperator extends InputOperator {

	/**
	 *  Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(AcquireOperator.class.getName());

    /** 
     * List of the attributes that are sensed by this operator. 
     * Excludes attributes such as time, id, EvalTime.
     * Includes attributes required for predicates.
     */
    private List<Attribute> sensedAttributes;
	
	/**
	 * Constructs a new Acquire operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param sources Metadata about data sources for the acquire extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public AcquireOperator(ExtentMetadata extentMetadata, 
			SourceMetadataAbstract source,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(extentMetadata, source, boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AcquireOperator() with " + 
					extentMetadata + " #source=" + source.getSourceName());
		}
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);
		updateSensedAttributes(); 
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN AcquireOperator()");
	} 

	/**
	 * Returns a string representation of the operator.
	 * @return Operator as a String.
	 */
	public String toString() {
		return this.getText();
	}

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		//if no project to push down Do nothing.
		if (projectAttributes.size() == 0) {
			return false;
		}

		if (projectExpressions.size() == 0) {
			//remove unrequired attributes. No expressions to accept
			for (int i = 0; i < outputAttributes.size(); ) {
				if (projectAttributes.contains(outputAttributes.get(i)) ||
						(outputAttributes.get(i) instanceof EvalTimeAttribute))
					i++;
				else {
					outputAttributes.remove(i);
					expressions.remove(i);		
				}
			}
			updateSensedAttributes();
			return false;
		}

		expressions = projectExpressions;
		outputAttributes = projectAttributes;
		
		updateInputAttributes();
		updateSensedAttributes();
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException {
		setPredicate(predicate);
		return true;
	}
	
	/** 
	 * Updates the sensed Attributes.
	 */	
	private void updateSensedAttributes() {
		sensedAttributes = new ArrayList<Attribute>();
		for (Attribute attr : inputAttributes) {
			if (attr instanceof DataAttribute) {
				sensedAttributes.add(attr);
			}
		}
	}	

	/**
	 * 	Extracts the attributes from the expressions or predicates
	 */
	private void updateInputAttributes() {
		for (int i = 0; i < expressions.size(); i++) {
			//DataAttribute sensed =  sensedAttributes.get(i);
			Expression expression = expressions.get(i);
			List<Attribute> attributes = 
				expression.getRequiredAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				Attribute attribute = attributes.get(j);
				if (!inputAttributes.contains(attribute)) {
					inputAttributes.add((DataAttribute) attribute);
				}
			}
		}
		List<Attribute> attributes = 
			getPredicate().getRequiredAttributes();
		for (int j = 0; j < attributes.size(); j++) {
			Attribute attribute = attributes.get(j);
			if (!inputAttributes.contains(attribute)) {
				inputAttributes.add((DataAttribute) attribute);
			}
		}
	}

	/**
	 * Get the list of attributes acquired/ sensed by this operator.
	 * List is before projection is pushed down.
	 * @return list of acquired attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public List<Attribute> getSensedAttributes() {
		assert (sensedAttributes != null);
		return sensedAttributes;
	}

	public int getNumSensedAttributes() {
		return sensedAttributes.size();
	}
	
	/**
	 * Converts an attribute into a reading number.
	 * @param attribute An Expression which must be of subtype Attribute
	 * @return A constant number for this attribute (starting at 0)
	 * @throws CodeGenerationException 
	 */
	public int getSensedAttributeNumber(Expression attribute)
	throws OptimizationException {
		assert (attribute instanceof DataAttribute);
		for (int i = 0; i < sensedAttributes.size(); i++) {
			if (attribute.equals(sensedAttributes.get(i))) {
				return i;
			}
		}
		throw new OptimizationException("Unable to find a number for attribute: " + attribute.toString());
	}
	
}
