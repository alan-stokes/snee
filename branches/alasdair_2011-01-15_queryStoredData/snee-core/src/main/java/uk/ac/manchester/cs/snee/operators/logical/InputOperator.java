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
	private List<SourceMetadataAbstract> sources;
	
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
			List<SourceMetadataAbstract> sources, 
			AttributeType boolType)
	throws SchemaMetadataException, TypeMappingException {
		super(boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER InputOperator() with " +
					extentMetadata + " #sources=" + sources.size());
		}
		extentName = extentMetadata.getExtentName();
		this.sources = sources;
		addMetadataInfo(extentMetadata);
		generateAndSetParamStr();
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
	private void generateAndSetParamStr() {
		StringBuffer sourcesStr = new StringBuffer(" sources={");
		boolean first = true;
		for (SourceMetadataAbstract sm : sources) {
			if (first) {
				first=false;
			} else {
				sourcesStr.append(",");
			}
			sourcesStr.append(sm.getSourceName());
		}
		sourcesStr.append("}");
		this.setParamStr(this.extentName + 
				" (cardinality=" + getCardinality(null) +
				sourcesStr);
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
	 * Return details of the data sources
	 * @return
	 */
	public List<SourceMetadataAbstract> getSources() {
		return sources;
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
		return inputAttributes.size();
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

	/** 
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
	}
	
}
