package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAggrEvalOperator extends SensornetIncrementalAggregationOperator {

	private static Logger logger 
	= Logger.getLogger(SensornetAggrEvalOperator.class.getName());
	
	public SensornetAggrEvalOperator(LogicalOperator op, CostParameters costParams) throws SNEEException,
			SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAggrEvalOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}	
		this.setNesCTemplateName("aggreval");
		this.setOperatorName("SensornetAGGREval");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAggrEvalOperator()");
		}		
	}
	
	/** {@inheritDoc} */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) {
		return getOverheadTimeCost()
			+ costParams.getDoCalculation()
			+ costParams.getCopyTuple();
    }
    
	//delegate except for exchange operators or aggregates
	public List<Attribute> getAttributes() {
		ArrayList<Attribute> outputAttributes = new ArrayList<Attribute>();
		
		try {
			outputAttributes.add(new EvalTimeAttribute());
		} catch (SchemaMetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		for (Attribute attr : this.getLogicalOperator().getAttributes()) {
			outputAttributes.add(attr);
		}
		return outputAttributes;
	}
	
	//delegate
	public boolean isAttributeSensitive() {
		return true;
	}
		
	//delegate
	public boolean isRecursive() {
		return false;
	}
}
