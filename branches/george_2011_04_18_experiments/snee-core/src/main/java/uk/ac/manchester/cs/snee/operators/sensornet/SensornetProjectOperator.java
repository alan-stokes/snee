package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;

public class SensornetProjectOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetProjectOperator.class.getName());
	
	ProjectOperator prjOp;
	
	public SensornetProjectOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetProjectOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		prjOp = (ProjectOperator) op;
		this.setNesCTemplateName("project");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetProjectOperator()");
		}		
	}

	//@Override
	/** {@inheritDoc} */
	public final int getCardinality(final CardinalityType card, 
			final Site node, final DAF daf) throws OptimizationException {
		return getInputCardinality(card, node, daf, 0);
	}

	//@Override
    /** {@inheritDoc} */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}

	//@Override
	/** {@inheritDoc} */    
    public final int getOutputQueueCardinality(final Site node, final DAF daf) 
	throws OptimizationException {
    	return super.defaultGetOutputQueueCardinality(node, daf);
    }
	
	/** {@inheritDoc} */
    public final int[] getSourceSites() {
    	return super.defaultGetSourceSites();
    }
	
    /** {@inheritDoc} 
     * @throws OptimizationException */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
		final int tuples = this.getInputCardinality(card, node, daf, 0);
		return getOverheadTimeCost()
			+ costParams.getCopyTuple() * tuples;
    }
}
