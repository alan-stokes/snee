package uk.ac.manchester.cs.snee.operators.logical;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.types.Duration;

public class ExchangeOperator extends LogicalOperatorImpl {

	private Logger logger = Logger.getLogger(this.getClass().getName());
	//Variable to hold how many objects should be pushed at once
	private int outputSize;
	private Duration queueScanInterval;
	

	public ExchangeOperator(LogicalOperator inputOperator, AttributeType boolType) {
		super(boolType);
		this.setOperatorName("EXCHANGE");
//		this.setNesCTemplateName("deliver");
		setChildren(new LogicalOperator[] {inputOperator});
		this.setOperatorDataType(inputOperator.getOperatorDataType());
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setSourceRate(inputOperator.getStreamRate());
		//TODO To be set through some configuration
		setOutputSize(5);
		//TODO setting the queueScannterval needs to be done through
		//some other intelligent mechanism like say from a property
		//file or dynamic configurations.
		setQueueScanInterval(new Duration(10));
	}

	@Override
	public List<Attribute> getAttributes() {
		return super.defaultGetAttributes();
	}

	public String getParamStr() {
		return "";
	}

	@Override
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

	@Override
	public int getCardinality(CardinalityType card) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isAttributeSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLocationSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean acceptsPredicates() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean pushProjectionDown(List<Expression> projectExpressions,
			List<Attribute> projectAttributes) throws OptimizationException,
			SNEEConfigurationException {
		// TODO Auto-generated method stub
		return getInput(0).pushProjectionDown(
				projectExpressions, projectAttributes);
	}

	@Override
	public boolean pushSelectIntoLeafOp(Expression predicate)
			throws SchemaMetadataException, AssertionError,
			TypeMappingException, SNEEConfigurationException {
		// TODO Auto-generated method stub
		return getInput(0).pushSelectIntoLeafOp(predicate);
	}

	@Override
	public boolean isRemoveable() {		
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		return this.getText() + " [ " + 
		super.getInput(0).toString() + " ]"; 
		
	}

	public void setOutputSize(int outputSize) {
		this.outputSize = outputSize;
	}

	public int getOutputSize() {
		return outputSize;
	}
	
	/**
	 * @param queueScanInterval the queueScanInterval to set
	 */
	public void setQueueScanInterval(Duration queueScanInterval) {
		this.queueScanInterval = queueScanInterval;
	}

	/**
	 * @return the queueScanInterval
	 */
	public Duration getQueueScanInterval() {
		return queueScanInterval;
	}

}
