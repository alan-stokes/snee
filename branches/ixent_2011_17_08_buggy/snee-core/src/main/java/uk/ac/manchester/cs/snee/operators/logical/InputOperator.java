package uk.ac.manchester.cs.snee.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
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
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public abstract class InputOperator extends LogicalOperatorImpl
implements LogicalOperator {

	private Logger logger = 
		Logger.getLogger(InputOperator.class.getName());
		
	/** List of input attributes */
	protected List<Attribute> inputAttributes;

	/**
	 *  Name of extent as found in the schema. 
	 */
	protected String extentName;

	/**
	 * Contains details of the data sources that contribute data
	 */
	private SourceMetadataAbstract source;

	/**
	 * For a sensor network, the cardinality is the number of source sites 
	 * in the sensor network providing data for this extent.
	 */
	private int cardinality;
	
	/**
	 *  List of attributes to be output. 
	 */
	protected List<Attribute> outputAttributes;

	/**
	 * The expressions for building the attributes.
	 * 
	 * Left in just in case receive will allowed project down.
	 */
	protected List<Expression> expressions;
	
	public InputOperator(ExtentMetadata extentMetadata,
			SourceMetadataAbstract source, 
			AttributeType boolType)
	throws SchemaMetadataException, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER InputOperator() with " +
					extentMetadata + " source=" + source.getSourceName());
		}
		extentName = extentMetadata.getExtentName();
		this.source = source;
		addMetadataInfo(extentMetadata);
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
					extentName);
		}
		inputAttributes = extentMetaData.getAttributes();
		outputAttributes = inputAttributes;
		this.cardinality = extentMetaData.getCardinality();
		copyExpressions(outputAttributes);
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
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
	 * Generates the parameter string that is used when displaying
	 * the query plan as a graph.
	 */
	public String getParamStr() {
		return this.extentName + 
				" (cardinality=" + getCardinality(null) +
				" source=" + source.getSourceName() + ")\n " + getPredicate();
	} 

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.operators.logical.LogicalOperatorImpl#toString()
	 * 
	 * Returns a string representation of the operator.
	 * @return Operator as a String.
	 */
	@Override
	public String toString() {
		return this.getText();
	}
	
	/**
	 * Return the name of the extent as it appears in the schema.
	 * @return
	 */
	public String getExtentName() {
		return extentName;
	}
	
	/**
	 * Return details of the data source for the extent provided by 
	 * this input operator.
	 * 
	 * @return SourceMetadata object representing the data source
	 */
	public SourceMetadataAbstract getSource() {
		return source;
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
	 * Calculate the cardinality based on the requested type. 
	 * 
	 * @param card Type of cardinality to be considered.
	 * 
	 * @return The Cardinality calculated as requested.
	 */
	public int getCardinality(CardinalityType card) {
		return cardinality;
	}

	/**
	 * Gets the attributes sensed by this source.
	 * This may include attributes needed for a predicate.
	 * @return Attributes that this source will sense.
	 */
	public List<Attribute> getInputAttributes() {
		assert (inputAttributes != null);
		return inputAttributes;
	}

	/** 
	 * Gets the number of attributes that are actually sensed.
	 * So excluded time/epoch and (site) id.
	 * @return Number of sensed attributes.
	 */
	public int getNumberInputAttributes() {
		return inputAttributes.size();
	}

	/**
	 * Converts an attribute into a reading number.
	 * @param attribute An Expression which must be of subtype Attribute
	 * @return A constant number for this attribute (starting at 1)
	 * @throws CodeGenerationException 
	 */
	public int getInputAttributeNumber(Expression attribute)
	throws OptimizationException {
		assert (attribute instanceof DataAttribute);
		for (int i = 0; i < inputAttributes.size(); i++) {
			if (attribute.equals(inputAttributes.get(i))) {
				return i;
			}
		}
		throw new OptimizationException("Unable to find a number for attribute: " + attribute.toString());
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushSelectIntoLeafOp(Expression predicate) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER pushSelectionIntoLeafOp() with " + predicate);
		}
		boolean boolSetting = true;
		boolean result = false;
		if (this instanceof AcquireOperator) {
			// May want to keep acquire and select separate for debugging nesC
			try {
				boolSetting = SNEEProperties.getBoolSetting(
						SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT);
			} catch (SNEEConfigurationException e) {
				logger.warn(e + " Proceeding with default value true.");
			}
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
			result = true;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN pushSelectionIntoLeafOp() with " + result);
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER pushProjectionDown() with " + projectExpressions +
					" " + projectAttributes);
		}
		boolean result = false;
		boolean boolSetting = true;
		if (this instanceof AcquireOperator) {
			// May want to keep acquire and project separate for debugging nesC
			try {
				boolSetting = SNEEProperties.getBoolSetting(
						SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT);
			} catch (SNEEConfigurationException e) {
				logger.warn(e + " Proceeding with default value true.");
			}
		}
		if (boolSetting) {					
			if (projectAttributes.isEmpty()) {
				//if no project to push down Do nothing.
				result = false;
			} else if (projectExpressions.isEmpty()) {
				//remove unrequired attributes. No expressions to accept
				for (int i = 0; i < outputAttributes.size(); ) {
					if (projectAttributes.contains(outputAttributes.get(i)))
						i++;
					else {
						outputAttributes.remove(i);
						expressions.remove(i);		
					}
				}
				updateInputAttributes();
				result = false;
			} else {
				expressions = projectExpressions;
				outputAttributes = projectAttributes;
				updateInputAttributes();
				result = true;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN pushProjectionDown() with " + result);
		}
		return result;
	}

	/** 
	 * Updates the input (sensed or scanned) Attributes.
	 * Extracts the attributes from the expressions.
	 * Those that are data attributes become the input attributes.
	 */	
	protected void updateInputAttributes() {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER updateInputAttributes()");
		}
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
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN updateInputAttributes()");
		}
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
	public boolean isLocationSensitive() {
		return true;
	}

	/** {@inheritDoc} */
	public boolean isRecursive() {
		return false;
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

	//XXX: Removed by AG as metadata now handled in metadata object
//	/** 
//	 * {@inheritDoc}
//	 * Should never be called as there is always a project or aggregation 
//	 * between this operator and the rename operator.
//	 */   
//	public void pushLocalNameDown(String newLocalName) {
//		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
//	}
	
}
