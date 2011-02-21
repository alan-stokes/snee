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
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class AcquireOperator extends LogicalOperatorImpl {

	/**
	 *  Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(AcquireOperator.class.getName());

	/**
	 *  Name of extent as found in the schema. 
	 */
	private String extentName;

	/**
	 *  List of attributes to be output. 
	 */
	private List<Attribute> outputAttributes;

	/**
	 * List of attributes to be sensed. 
	 */
	private List<Attribute> sensedAttributes;

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
	 * The expressions for building the attributes.
	 */
	private List <Expression> expressions;

	/**
	 * Contains metadata information about which sources contribute
	 * data via an acquire mechanism
	 */
	private SourceMetadataAbstract _source;
	
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
	 * @param sources Metadata about data sources for the acquire extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public AcquireOperator(ExtentMetadata extentMetadata, 
			Types types, 
			SourceMetadataAbstract source,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AcquireOperator() with " + 
					extentMetadata + " #source=" + source.getSourceName());
		}
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);
		this._types=types;
		this.extentName = extentMetadata.getExtentName();
		this._source = source;		
		
		addMetadataInfo(extentMetadata);
		updateSensedAttributes(); 
		
		StringBuffer sourcesStr = new StringBuffer(" source={");
		sourcesStr.append(_source.getSourceName());
		sourcesStr.append("}");
		this.setParamStr(this.extentName + 
				" (cardinality=" + this.cardinality +
				sourcesStr);
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN AcquireOperator()");
	} 

	/**
	 * Return the name of the extent as it appears in the schema.
	 * @return
	 */
	public String getExtentName() {
		return extentName;
	}

	/**
	 * Return details of the data sources
	 * @return
	 */
	public SourceMetadataAbstract getSource() {
		return _source;
	}

	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetadata DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addMetadataInfo(ExtentMetadata extentMetadata) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addMetaDataInfo() with " +
					extentMetadata);
		}
		outputAttributes = new ArrayList<Attribute>();
//		outputAttributes.add(new EvalTimeAttribute(extentName, 
//				Constants.EVAL_TIME,
//				_types.getType(Constants.TIME_TYPE))); 
//		outputAttributes.add(new TimeAttribute(extentName,
//				Constants.ACQUIRE_TIME, 
//				_types.getType(Constants.TIME_TYPE)));
//		outputAttributes.add(new IDAttribute(extentName, 
//				Constants.ACQUIRE_ID,
//				_types.getType("integer")));
//TODO: Localtime
//		if (Settings.CODE_GENERATION_SHOW_LOCAL_TIME) {
//			outputAttributes.add(new LocalTimeAttribute()); //Ixent added this
//		}		
		sensedAttributes = extentMetadata.getAttributes();
		outputAttributes.addAll(sensedAttributes);
//		sites =  sourceMetaData.getSourceNodes();
		this.cardinality = extentMetadata.getCardinality();
		copyExpressions(outputAttributes);
		acquiredAttributes = (ArrayList<Attribute>) outputAttributes;
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
	}

	/**
	 * Returns a string representation of the operator.
	 * @return Operator as a String.
	 */
	public String toString() {
		return this.getText();
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

	/**
	 * Used to determine if the operator is Attribute sensitive.
	 * 
	 * @return false.
	 */
	public boolean isAttributeSensitive() {
		return false;
	}

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		//logger.warn("Acquire does not yet accept predicates");
		//return true;
//		return Settings.LOGICAL_OPTIMIZATION_COMBINE_ACQUIRE_AND_SELECT;
		return true;
	}

	/** {@inheritDoc} */
	public boolean isLocationSensitive() {
		return true;
	}

	/** {@inheritDoc} */
	public boolean isRecursive() {
		return false;
	}

	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */ 
	public List<Attribute> getAttributes() {
		return outputAttributes;
	}

	/** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return expressions;
	}

	/**
	 * Copies attributes into the expressions.
	 * @param attributes Values to set them to.
	 */
	protected void copyExpressions(List<Attribute> attributes) {
		expressions = new ArrayList<Expression>(); 
		expressions.addAll(attributes);
	}

	/** 
	 * Updates the sensed Attributes.
	 * Extracts the attributes from the expressions.
	 * Those that are data attributes become the sensed attributes.
	 */	
	private void updateSensedAttributes() {
		sensedAttributes = new ArrayList<Attribute>();
		for (int i = 0; i < expressions.size(); i++) {
			//DataAttribute sensed =  sensedAttributes.get(i);
			Expression expression = expressions.get(i);
			List<Attribute> attributes = 
				expression.getRequiredAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				Attribute attribute = attributes.get(j);
				if (attribute instanceof DataAttribute) {
					if (!sensedAttributes.contains(attribute)) {
						sensedAttributes.add((DataAttribute) attribute);
					}
				}
			}
		}
		List<Attribute> attributes = 
			getPredicate().getRequiredAttributes();
		for (int j = 0; j < attributes.size(); j++) {
			Attribute attribute = attributes.get(j);
			if (attribute instanceof DataAttribute) {
				if (!sensedAttributes.contains(attribute)) {
					sensedAttributes.add((DataAttribute) attribute);
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
		if (projectAttributes.size() == 0) {
			return false;
		}

		if (projectExpressions.size() == 0) {
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
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		//XXX-AG: Commented out method body
		/*
		 * This method was being used to relabel an extent in a 
		 * query. Apparently it should never be called, so
		 * why do we have it? Have commented it out!
		 */
//		localName = newLocalName;
//		for (int i = 0; i<outputAttributes.size(); i++){
//			outputAttributes.get(i).setLocalName(newLocalName);
//		}
	}

	/**
	 * Some operators do not change the data in any way those could be removed.
	 * This operator does change the data so can not be. 
	 * 
	 * @return False. 
	 */
	public boolean isRemoveable() {
		return false;
	}

	/**
	 * Gets the attributes sensed by this source.
	 * This may include attributes needed for a predicate.
	 * @return Attributes that this source will sense.
	 */
	public List<Attribute> getSensedAttributes() {
		assert (sensedAttributes != null);
		return sensedAttributes;
	}

	/**
	 * Converts an attribute into a reading number.
	 * @param attribute An Expression which must be of subtype Attribute
	 * @return A constant number for this attribute (starting at 1)
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

	/** 
	 * Gets the number of attributes that are actually sensed.
	 * So excluded time/epoch and (site) id.
	 * @return Number of sensed attributes.
	 */
	public int getNumSensedAttributes() {
		return sensedAttributes.size();
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
