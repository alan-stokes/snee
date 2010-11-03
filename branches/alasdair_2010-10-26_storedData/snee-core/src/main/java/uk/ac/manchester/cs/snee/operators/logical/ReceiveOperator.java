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
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class ReceiveOperator extends LogicalOperatorImpl {

	/**
	 *  Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(ReceiveOperator.class.getName());

	/**
	 *  Name of extent as found in the schema. 
	 */
	private String extentName;

	/**
	 *  List of attributes to be output. 
	 */
	private List<Attribute> outputAttributes;

	/** List of attributes to be received. */
	private List<Attribute> receivedAttributes;

	/**
	 * The expressions for building the attributes.
	 * 
	 * Left in just in case receive will allowed project down.
	 */
	private List <Expression> expressions;

	/**
	 * Contains details of the data sources that contribute data
	 */
	private List<SourceMetadataAbstract> _sources;
	
	/**
	 * Constructs a new Receive operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param sources Metadata about data sources for the receive extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public ReceiveOperator(ExtentMetadata extentMetaData, 
			List<SourceMetadataAbstract> sources, 
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ReceiveOperator() with " +
					extentMetaData + " #sources=" + sources.size());
					}
		this.setOperatorName("RECEIVE");
		//        this.setNesCTemplateName("receive");
		this.setOperatorDataType(OperatorDataType.STREAM);

		this.extentName = extentMetaData.getExtentName();
		this._sources = sources;
		addMetadataInfo(extentMetaData);
		
		StringBuffer sourcesStr = new StringBuffer(" sources={");
		boolean first = true;
		for (SourceMetadataAbstract sm : _sources) {
			if (first) {
				first=false;
			} else {
				sourcesStr.append(",");
			}
			sourcesStr.append(sm.getSourceName());
		}
		sourcesStr.append("}");
		this.setParamStr(this.extentName + sourcesStr);
			
		if (logger.isDebugEnabled())
			logger.debug("RETURN ReceiveOperator()");
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
	public List<SourceMetadataAbstract> getSources() {
		return _sources;
	}
	
	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetaData DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addMetadataInfo(ExtentMetadata extentMetaData) 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addMetaDataInfo() with " +
					extentMetaData);
		}
		//This version assumes input does not have a timestamp
		receivedAttributes = extentMetaData.getAttributes();
		outputAttributes = receivedAttributes;
		copyExpressions(outputAttributes);
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
		return receivedAttributes.size();
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
		logger.warn("Receive does not accept predicates");
		return false;
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
	protected void copyExpressions(
			List<Attribute> attributes) {
		expressions = new ArrayList<Expression>(); 
		expressions.addAll(attributes);
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
		//setPredicate(predicate);
		//return true;
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
	public List<Attribute> getReceivedAttributes() {
		assert (receivedAttributes != null);
		return receivedAttributes;
	}

	/**
	 * Converts an attribute into a reading number.
	 * @param attribute An Expression which must be of subtype Attribute
	 * @return A constant number for this attribute (starting at 1)
	 */
	public int getReceivedAttributeNumber(Expression attribute) {
		assert (attribute instanceof DataAttribute);
		for (int i = 0; i < receivedAttributes.size(); i++) {
			if (attribute.equals(receivedAttributes.get(i))) {
				return i;
			}
		}
		//XXX-AG: Shouldn't this throw an exception?
		return 1;
	}

	/** 
	 * Gets the number of attributes that are actually sensed.
	 * So excluded time/epoch and (site) id.
	 * @return Number of sensed attributes.
	 */
	public int getNumReceivedAttributes() {
		return receivedAttributes.size();
	}

	/**
	 * Get the list of attributes received by this operator.
	 * List is before projection is pushed down.
	 * @return list of received attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public List<Attribute> getAllReceivedAttributes() 
	throws SchemaMetadataException, TypeMappingException {
		return receivedAttributes;
	}

}
