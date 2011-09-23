package uk.ac.manchester.cs.snee.operators.sensornet;


import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAcquireOperator extends SensornetOperatorImpl {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 4424935445543873310L;

  private static final Logger logger = Logger.getLogger(SensornetAcquireOperator.class.getName());
	
	AcquireOperator acqOp;
	
	SensorNetworkSourceMetadata sourceMetadata;
  
	public SensornetAcquireOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException,
			SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAcquireOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		acqOp = (AcquireOperator) op;
		this.setNesCTemplateName("acquire");
		//TODO: Separate acquire operator will be need for each source?
		this.sourceMetadata = (SensorNetworkSourceMetadata) 
			this.acqOp.getSource();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAcquireOperator()");
		}		
	}

  @Override
	public int[] getSourceSites() {
		return this.sourceMetadata.getSourceSites(acqOp.getExtentName());
	}

	/** {@inheritDoc} */
	public int getOutputQueueCardinality(Site node, DAF daf) {
		return this.getCardinality(CardinalityType.PHYSICAL_MAX, node, daf);
	}

	/**
	 * The physical maximum size of the output.
	 * 
	 * Each AcquireOperator on a single site 
	 * 		returns exactly 1 tuple per evaluation. 
	 *
	 * @param card Ignored.
	 * @param node Ignored
	 * @param daf Ignored
	 * @return 1
	 */
	public int getCardinality(CardinalityType card, Site node, DAF daf) {
		return 1;
	}

	/** {@inheritDoc} 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException */    
	public int getDataMemoryCost(Site node, DAF daf)
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}

	/** {@inheritDoc} */
	private double getTimeCost() {
		logger.trace("" + costParams.getSignalEvent());
		logger.trace("" + costParams.getAcquireData());
		return getOverheadTimeCost()
		+ costParams.getAcquireData()
		+ costParams.getCopyTuple() + costParams.getSetAValue()
		+ costParams.getApplyPredicate();
	}

	/** {@inheritDoc} */
	public double getTimeCost(CardinalityType card, 
			Site node, DAF daf) {
		return getTimeCost();
	}

	//delegate except for exchange operators or incremental aggregates
	public List<Attribute> getAttributes() {
		return this.getLogicalOperator().getAttributes();
	}
	
	//delegate
	public String getExtentName() {
		return acqOp.getExtentName();
	}

}
