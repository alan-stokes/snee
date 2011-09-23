package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetDeliverOperator extends SensornetOperatorImpl {
	
	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2427342824331768901L;

  private static final Logger logger = Logger.getLogger(SensornetDeliverOperator.class.getName());
	
	DeliverOperator delOp;
	
	public SensornetDeliverOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetDeliverOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		delOp = (DeliverOperator) op;
		this.setNesCTemplateName("deliver");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetDeliverOperator()");
		}		
	}

  /** {@inheritDoc} 
	 * @throws OptimizationException */
	public final int getCardinality(final CardinalityType card, 
			final Site node, final DAF daf) throws OptimizationException {
		return getInputCardinality(card, node, daf, 0);
	}
	
	/** {@inheritDoc} 
	 * @throws OptimizationException */    
    public final int getOutputQueueCardinality(final Site node, final DAF daf) throws OptimizationException {
    	return super.defaultGetOutputQueueCardinality(node, daf);
    }
    
    /** {@inheritDoc} 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}

	@Override
    /** {@inheritDoc} */
    public final int[] getSourceSites() {
    	return super.defaultGetSourceSites();
    }
	
	@Override
	/** {@inheritDoc} */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
		final int tuples 
			= this.getInputCardinality(card, node, daf, 0);
		final int packets = packetsPerTuple() * tuples;
		double duration = getOverheadTimeCost()
			+ costParams.getDeliverTuple() * packets;
		return duration;
    }
	
    /**
     * Objains the size of the String needed to represent this tuple.
     * @return Output String size in bytes
     */
    private final int packetsPerTuple() {
		int tupleSize = costParams.getDeliverOverhead();
		final List<Attribute> attributes = this.getLogicalOperator().getAttributes(); 
		for (int i = 0; i < attributes.size(); i++) {
			String attrName = getDeliverName(attributes.get(i));			
			tupleSize += attrName.length() + costParams.getAttributeStringLength();
			logger.trace("TuplesSize now " + tupleSize);
		}
		return (int) Math.ceil(tupleSize / costParams.getDeliverPayloadSize());
    }

	/**
	 * Generates the name Deliver should use for this attribute. 
	 * @param attr The Attribute a name should be generated for.
	 * @return The String to used by deliver.
	 */
	private static String getDeliverName(final Attribute attr) {
		if (attr instanceof EvalTimeAttribute) {
			return "evalEpoch";
		}
		if (attr instanceof TimeAttribute) {
			return attr.getAttributeDisplayName() + ("Epoch");
		}
		return attr.getAttributeDisplayName(); 
	}
	
}
