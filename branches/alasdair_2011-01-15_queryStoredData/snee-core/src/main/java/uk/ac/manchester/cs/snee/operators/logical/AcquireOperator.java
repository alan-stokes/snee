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

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IDAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
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
     * List of the attributes that are acquired by this operator. 
     * Includes sensed attributes as well as time, id 
     * and any control attributes such as EvalTime.
     * Includes attributes required for predicates.
     * All attributes are in the original format 
     * before any expressions are applied.
     */
    private List<Attribute> acquiredAttributes;
	
	/**
	 * Number of source sites in the sensor network providing data for this extent.
	 */
	private int cardinality;

	/**
	 * Metadata about the types supported.
	 */
	Types _types;
	
	/**
	 * Constructs a new Acquire operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param types type information as read in from the types file
	 * @param sources Metadata about data sources for the acquire extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public AcquireOperator(ExtentMetadata extentMetadata, 
			Types types, 
			List<SourceMetadataAbstract> sources,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(extentMetadata, sources, boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AcquireOperator() with " + 
					extentMetadata + " #sources=" + sources.size());
		}
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);
		this._types=types;
		updateSensedAttributes(); 
		updateMetadataInfo(extentMetadata);
		if (logger.isDebugEnabled())
			logger.debug("RETURN AcquireOperator()");
	}

	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetaData DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void updateMetadataInfo(ExtentMetadata extentMetaData) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addMetaDataInfo() with " +
					extentMetaData);
		}
		outputAttributes = new ArrayList<Attribute>();
		outputAttributes.add(new EvalTimeAttribute(extentName, 
				Constants.EVAL_TIME,
				_types.getType(Constants.TIME_TYPE))); 
		outputAttributes.add(new TimeAttribute(extentName,
				Constants.ACQUIRE_TIME, 
				_types.getType(Constants.TIME_TYPE)));
		outputAttributes.add(new IDAttribute(extentName, 
				Constants.ACQUIRE_ID,
				_types.getType("integer")));
//TODO: Localtime
//		if (Settings.CODE_GENERATION_SHOW_LOCAL_TIME) {
//			outputAttributes.add(new LocalTimeAttribute()); //Ixent added this
//		}		
		inputAttributes = extentMetaData.getAttributes();
		outputAttributes.addAll(inputAttributes);
//		sites =  sourceMetaData.getSourceNodes();
		this.cardinality = extentMetaData.getCardinality();
		copyExpressions(outputAttributes);
		acquiredAttributes = (ArrayList<Attribute>) outputAttributes;
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
	}

	/**
	 * Calculated the cardinality based on the requested type. 
	 * 
	 * @param card Type of cardinality to be considered.
	 * 
	 * @return The Cardinality calculated as requested.
	 */
	public int getCardinality(CardinalityType card) {
		return this.cardinality;
	}

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		//logger.warn("Acquire does not yet accept predicates");
		//return true;
//		return Settings.LOGICAL_OPTIMIZATION_COMBINE_ACQUIRE_AND_SELECT;
		return true;
	}

	/** 
	 * Updates the sensed Attributes.
	 * Extracts the attributes from the expressions.
	 * Those that are data attributes become the sensed attributes.
	 */	
	private void updateSensedAttributes() {
		inputAttributes = new ArrayList<Attribute>();
		for (int i = 0; i < expressions.size(); i++) {
			//DataAttribute sensed =  sensedAttributes.get(i);
			Expression expression = expressions.get(i);
			List<Attribute> attributes = 
				expression.getRequiredAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				Attribute attribute = attributes.get(j);
				if (attribute instanceof DataAttribute) {
					if (!inputAttributes.contains(attribute)) {
						inputAttributes.add((DataAttribute) attribute);
					}
				}
			}
		}
		List<Attribute> attributes = 
			getPredicate().getRequiredAttributes();
		for (int j = 0; j < attributes.size(); j++) {
			Attribute attribute = attributes.get(j);
			if (attribute instanceof DataAttribute) {
				if (!inputAttributes.contains(attribute)) {
					inputAttributes.add((DataAttribute) attribute);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		//if no project to push down Do nothing.
		if (projectAttributes.isEmpty()) {
			return false;
		}

		if (projectExpressions.isEmpty()) {
			//remove unrequired attributes. No expressions to accept
			for (int i = 0; i < outputAttributes.size(); ) {
				if (projectAttributes.contains(outputAttributes.get(i)))
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
		updateSensedAttributes();
//		if (Settings.CODE_GENERATION_SHOW_LOCAL_TIME) {
//			try {
//				outputAttributes.add(new LocalTimeAttribute());
//				expressions.add(new LocalTimeAttribute());
//			} catch (SchemaMetadataException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				System.exit(2);
//			} //Ixent added this
//		}		
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
	 * Get the list of attributes acquired/ sensed by this operator.
	 * List is before projection is pushed down.
	 * @return list of acquired attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public List<Attribute> getAcquiredAttributes() 
	throws SchemaMetadataException, TypeMappingException {
		assert (acquiredAttributes != null);
		return acquiredAttributes;
	}

}
