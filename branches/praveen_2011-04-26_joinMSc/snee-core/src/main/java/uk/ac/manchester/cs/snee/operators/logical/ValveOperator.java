package uk.ac.manchester.cs.snee.operators.logical;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.types.Duration;

public class ValveOperator extends LogicalOperatorImpl {

	/**
	 * The first mode of implementation of Valve operator,
	 * (A simple queue, which grows in size when there is no space available
	 * for the new objects)
	 */
	public static final String GROW_SIZE_MODE = "GROW_SIZE_MODE";
	/**
	 * The second mode of implementation of Valve operator,
	 * (A Circular Array which drops the oldest object in the queue by overwriting them)
	 */
	public static final String TUPLE_DROP_MODE = "TUPLE_DROP_MODE";	
	/**
	 * The third mode of implementation of Valve operator,
	 * (A modified Circular Array which instead of rewriting the oldest with the newest, 
	 * swaps the old data to the disk and then overwrites the array with the new data. 
	 * If no space is available for swapping to disk then overwrite as usual for 
	 * the circular array)
	 */
	public static final String TUPLE_OFFLOAD_MODE = "TUPLE_OFFLOAD_MODE";
	private Logger logger = Logger.getLogger(this.getClass().getName());	
	private Duration queueScanInterval;
	private boolean isPushBasedOperator;
	//Variable to hold how many objects should be pushed at once
	private int outputSize;
	//This variable holds the algorithm or the mode in which the
	//valve operator would work
	private String algorithm;
	

	public ValveOperator(LogicalOperator inputOperator, AttributeType boolType) {
		super(boolType);
		this.setOperatorName("VALVE");
		//TODO setting the queueScannterval needs to be done through
		//some other intelligent mechanism like say from a property
		//file or dynamic configurations.
		setQueueScanInterval(new Duration(10));
		//By default the valve operator is assumed to operate in Push
		//mode unless otherwise implied
		setPushBasedOperator(true);
		//TODO To be set through some configuration
		setOutputSize(5);
		//Setting the default algorithm as the Grow in Size 
		//when more tuples arrive
		setAlgorithm(GROW_SIZE_MODE);
//		this.setNesCTemplateName("deliver");
		setChildren(new LogicalOperator[] {inputOperator});
		this.setOperatorDataType(inputOperator.getOperatorDataType());
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setSourceRate(inputOperator.getSourceRate());
		this.setParamStr("");
		
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Attribute> getAttributes() {
		return super.defaultGetAttributes();
	}

	@Override
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

	@Override
	public int getCardinality(CardinalityType card) {
		return (this.getInput(0)).getCardinality(card);
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
	public boolean isRecursive() {
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
			List<Attribute> projectAttributes) throws OptimizationException {
		// TODO Auto-generated method stub
		return getInput(0).pushProjectionDown(
				projectExpressions, projectAttributes);
	}

	@Override
	public boolean pushSelectDown(Expression predicate)
			throws SchemaMetadataException, AssertionError,
			TypeMappingException {
		// TODO Auto-generated method stub
		return getInput(0).pushSelectDown(predicate);
	}

	@Override
	public boolean isRemoveable() {		
		// TODO Auto-generated method stub
		return false;
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

	/**
	 * @param isPushBasedOperator the isPushBasedOperator to set
	 */
	public void setPushBasedOperator(boolean isPushBasedOperator) {
		this.isPushBasedOperator = isPushBasedOperator;
	}

	/**
	 * @return the isPushBasedOperator
	 */
	public boolean isPushBasedOperator() {
		return isPushBasedOperator;
	}

	public void setOutputSize(int outputSize) {
		this.outputSize = outputSize;
	}

	public int getOutputSize() {
		return outputSize;
	}

	@Override
	public String toString() {
		return this.getText() + " [ " + 
		super.getInput(0).toString() + " ]"; 
		
	}
	
	/**
	 * Method to set the chosen algorithm
	 * 
	 * @param chosenAlgorithm
	 */
	public void setAlgorithm(String chosenAlgorithm) {
		this.algorithm = chosenAlgorithm;
	}
	
	/**
	 * Method to get the chosen algorithm
	 * @return
	 */
	public String getAlgorithm() {
		return this.algorithm;
	}

}
