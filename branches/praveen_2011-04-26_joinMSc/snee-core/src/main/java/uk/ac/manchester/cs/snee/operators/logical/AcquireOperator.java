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

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.StreamingSourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;

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
	 * Metadata about the types supported.
	 */
	Types _types;
	
	/**
	 * Constructs a new Acquire operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param types type information as read in from the types file
	 * @param source Metadata about data sources for the acquire extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 * @throws SourceMetadataException 
	 */
	public AcquireOperator(ExtentMetadata extentMetadata, 
			Types types, 
			SourceMetadataAbstract source,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException, SourceMetadataException {
		super(extentMetadata, source, boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AcquireOperator() with " + 
					extentMetadata + " source=" + source.getSourceName());
		}
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);
		this.setOperatorSourceType(source.getSourceType());
		//this.setSourceRate(((SourceMetadata)source).getRate(extentName));
		this._types=types;
		updateSensedAttributes(); 
		updateMetadataInfo(extentMetadata);
		if (logger.isDebugEnabled())
			logger.debug("RETURN AcquireOperator()");
	}

	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetadata DDL declaration for this extent.
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
//TODO: Localtime
//		if (Settings.CODE_GENERATION_SHOW_LOCAL_TIME) {
//			outputAttributes.add(new LocalTimeAttribute()); //Ixent added this
//		}		
		inputAttributes = extentMetaData.getAttributes();
		outputAttributes.addAll(inputAttributes);
//		sites =  sourceMetaData.getSourceNodes();
		copyExpressions(outputAttributes);
		acquiredAttributes = (ArrayList<Attribute>) outputAttributes;
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
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
	 * @throws SNEEConfigurationException 
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		boolean boolSetting = true;
		try {
			boolSetting = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT);
		} catch (SNEEConfigurationException e) {
			logger.warn(e + " Proceeding with default value true.");
		}
		if (boolSetting) {			
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
		} else {
			// Projection should be kept separate from acquire for debugging
			return false;
		}
	}

	/**
	 * {@inheritDoc}
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 * @throws SNEEConfigurationException 
	 */
	public boolean pushSelectIntoLeafOp(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER pushSelectionIntoLeafOp() with " + predicate);
		}
		boolean boolSetting = true;
		try {
			boolSetting = SNEEProperties.getBoolSetting(
					SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT);
		} catch (SNEEConfigurationException e) {
			logger.warn(e + " Proceeding with default value true.");
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Combine acquire and select=" + boolSetting);
		}
		if (boolSetting) {	
			Expression myPredicate = this.getPredicate();
			if (myPredicate instanceof NoPredicate) {
				setPredicate(predicate);
			} else {
				Expression[] expArray = new Expression[2];
				expArray[0] = myPredicate;
				expArray[1] = predicate;
				setPredicate(new MultiExpression(expArray, MultiType.AND, 
						_boolType));
			}
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN pushSelectionIntoLeafOp() with true");
			}
			return true;
		} else {
			// Selection should be kept separate from acquire for debugging
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN pushSelectionIntoLeafOp() with false");
			}
			return false;
		}
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

	public String getParamStr() {
		return this.extentName + 
		" (cardinality=" + getCardinality(null) +
		" source=" + this.getSource().getSourceName() + ")\n " + 
		getPredicate() + "\n" +
		this.getExpressions().toString();
	}

}
