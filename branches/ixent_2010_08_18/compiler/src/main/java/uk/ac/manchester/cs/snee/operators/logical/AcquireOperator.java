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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;

public class AcquireOperator extends LogicalOperatorImpl {

	/** Standard Java Logger. */
	private Logger logger = 
		Logger.getLogger(AcquireOperator.class.getName());

	/** Name as found in the DDL. */
	private String extentName;

	/** Name as found in the Query. */
	private String localName;

	/** List of attributes to be output. */
	private List<Attribute> outputAttributes;

	/** List of attributes to be sensed. */
	private ArrayList<DataAttribute> sensedAttributes;

	/**
	 * The expressions for building the attributes.
	 */
	private List <Expression> expressions;

	/**
	 * Contains metadata information about which sources contribute
	 * data via an acquire mechanism
	 */
	private List<SourceMetadata> _sources;
	
	private int cardinality;

	/**
	 * Constructs a new Acquire operator.
	 * 
	 * @param extentName Global name for this source
	 * @param localName Name for this source within this query or subquery
	 * @param schemaMetadata The DDL Schema
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public AcquireOperator(String extentName, String localName,  
			ExtentMetadata extentMetadata, List<SourceMetadata> sources,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled())
			logger.debug("ENTER AcquireOperator() with " + extentName + 
					" " + localName);
		
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);

		this.extentName = extentName;
		this.localName = localName;
		addMetaDataInfo(extentMetadata);
		updateSensedAttributes(); 
		
		_sources = sources;
		StringBuffer sourcesStr = new StringBuffer();
		boolean first = true;
		for (SourceMetadata sm : _sources) {
			if (first)
				first=false;
			else
				sourcesStr.append(",");
			sourcesStr.append(sm.getSourceName());
		}		
		this.setParamStr(this.extentName + " (card=" + this.cardinality +
				" sources={"+sourcesStr+"})");
		
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
	 * Return the name of the extent as it is referenced in the query.
	 * @return
	 */
	public String getQueryName() {
		return localName;
	}
	
	/**
	 * Return details of the data sources
	 * @return
	 */
	public List<SourceMetadata> getSources() {
		return _sources;
	}

	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetaData DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addMetaDataInfo(ExtentMetadata extentMetaData) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER addMetaDataInfo() with " + extentMetaData);
		outputAttributes = new ArrayList<Attribute>();
//		outputAttributes.add(new EvalTimeAttribute(_types.getType(Constants.TIME_TYPE))); 
//		outputAttributes.add(new TimeAttribute(localName, _types.getType(Constants.TIME_TYPE)));
//		outputAttributes.add(new IDAttribute(localName, _types.getType("integer")));
		sensedAttributes = new ArrayList<DataAttribute>();
		Map<String, AttributeType> typeMap = extentMetaData.getAttributes();
		String[] attributeNames = new String[1];
		attributeNames = typeMap.keySet().toArray(attributeNames);
		DataAttribute attribute;
		int numAttributes = typeMap.size();
		for (int i = 0; i < numAttributes; i++) {
			if (!(attributeNames[i].equals(Constants.ACQUIRE_TIME) 
					|| attributeNames[i].equals(Constants.ACQUIRE_ID))) {
				AttributeType type = typeMap.get(attributeNames[i]);
				attribute = new DataAttribute(
						localName, attributeNames[i], type);
				outputAttributes.add(attribute);
				sensedAttributes.add(attribute);
//				sites =  sourceMetaData.getSourceNodes();
			}
		}
		this.cardinality = extentMetaData.getCardinality();
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
		logger.warn("Acquire does not yet accept predicates");
		//return true;
//		return Settings.LOGICAL_OPTIMIZATION_COMBINE_ACQUIRE_AND_SELECT;
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
		sensedAttributes = new ArrayList<DataAttribute>();
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
		localName = newLocalName;
		for (int i = 0; i<outputAttributes.size(); i++){
			outputAttributes.get(i).setLocalName(newLocalName);
		}
	}

//	/** {@inheritDoc} */
//	public int getOutputQueueCardinality(Site node, DAF daf) {
//		return this.getCardinality(CardinalityType.PHYSICAL_MAX, node, daf);
//	}

//	/** {@inheritDoc} */
//	public int getOutputQueueCardinality(int numberOfInstances) {
//		assert(numberOfInstances == sites.length);
//		return this.getCardinality(CardinalityType.PHYSICAL_MAX);
//	}

//	/**
//	 * The physical maximum size of the output.
//	 * 
//	 * Each AcquireOperator on a single site 
//	 * 		returns exactly 1 tuple per evaluation. 
//	 *
//	 * @param card Ignored.
//	 * @param node Ignored
//	 * @param daf Ignored
//	 * @return 1
//	 */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		if (Settings.MEASUREMENTS_MULTI_ACQUIRE >= 0) {
//			return Settings.MEASUREMENTS_MULTI_ACQUIRE;			
//		} else {
//			return 1;
//		}
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getCardinality(CardinalityType card, 
//			Site node, DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		if (Settings.MEASUREMENTS_MULTI_ACQUIRE >= 0) {
//			result.addBetaTerm(Settings.MEASUREMENTS_MULTI_ACQUIRE);
//		} else {
//			result.addBetaTerm(1);
//		}
//		return result;
//	}

//	/** {@inheritDoc} */
//	private double getTimeCost() {
//		logger.trace("" + CostParameters.getSignalEvent());
//		logger.trace("" + CostParameters.getAcquireData());
//		return getOverheadTimeCost()
//		+ CostParameters.getAcquireData()
//		+ CostParameters.getCopyTuple() + CostParameters.getSetAValue()
//		+ CostParameters.getApplyPredicate();
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, 
//			Site node, DAF daf) {
//		return getTimeCost();
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, int numberOfInstances){
//		assert(numberOfInstances == sites.length);
//		return getTimeCost();		
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		return new AlphaBetaExpression(getTimeCost(card, node, daf),0);
//	}

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
	public List<DataAttribute> getSensedAttributes() {
		assert (sensedAttributes != null);
		return sensedAttributes;
	}

	/**
	 * Converts an attribute into a reading number.
	 * @param attribute An Expression which must be of subtype Attribute
	 * @return A constant number for this attribute (starting at 1)
	 */
	public int getSensedAttributeNumber(Expression attribute) {
		assert (attribute instanceof DataAttribute);
		for (int i = 0; i < sensedAttributes.size(); i++) {
			if (attribute.equals(sensedAttributes.get(i))) {
				return i;
			}
		}
//		Utils.handleCriticalException(new CodeGenerationException(
//				"Unable to find a number for attribute: " 
//				+ attribute.toString()));
		return 1;
	}

	/** 
	 * Gets the number of attributes that are actually sensed.
	 * So excluded time/epoch and (site) id.
	 * @return Number of sensed attributes.
	 */
	public int getNumSensedAttributes() {
		return sensedAttributes.size();
	}

//	/** {@inheritDoc} */    
//	public int getDataMemoryCost(Site node, DAF daf) {
//		return super.defaultGetDataMemoryCost(node, daf);
//	}

	/**
	 * Get the list of attributes acquired/ sensed by this operator.
	 * List is before projection is pushed down.
	 * @return list of acquired attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public List<Attribute> getAcquiredAttributes() 
	throws SchemaMetadataException, TypeMappingException {
		List<Attribute> acquiredAttributes = 
			(ArrayList<Attribute>)sensedAttributes.clone();
//		acquiredAttributes.add(new EvalTimeAttribute(
//				_types.getType(Constants.TIME_TYPE)));
//		acquiredAttributes.add(new TimeAttribute(localName, 
//				_types.getType(Constants.TIME_TYPE)));
//		acquiredAttributes.add(new IDAttribute(localName,
//				_types.getType("integer")));
		return acquiredAttributes;
	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf) {
//		return SharedCostFunctions.dAcquire(sensedAttributes.size(), 
//				getPredicate(), expressions); 
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf) {
//		return SharedCostFunctions.eAcquire(sensedAttributes.size(), 
//				getPredicate(), expressions); 
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int getDataMemoryCost2(Site node, DAF daf) {
//		return SharedCostFunctions.mAcquire(sensedAttributes.size(), expressions);
//	}


}
