package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAggrInitOperator extends SensornetIncrementalAggregationOperator {

	private static Logger logger 
	= Logger.getLogger(SensornetAggrInitOperator.class.getName());
	
	public SensornetAggrInitOperator(LogicalOperator op, CostParameters costParams) throws SNEEException,
			SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAggrInitOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}	
		this.setNesCTemplateName("aggrinit");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAggrInitOperator()");
		}		
	}

    /** {@inheritDoc} 
     * @throws OptimizationException */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
		//logger.finest("started");
		//logger.finest("input = " + this.getInput(0).getClass());
		final int tuples 
			= ((SensornetOperator)this.getInput(0)).getCardinality(card, node, daf);
		//logger.finest("tuples =" + tuples);
		return getOverheadTimeCost()
			+ costParams.getDoCalculation() * tuples
			+ costParams.getCopyTuple();
    }
	
}
