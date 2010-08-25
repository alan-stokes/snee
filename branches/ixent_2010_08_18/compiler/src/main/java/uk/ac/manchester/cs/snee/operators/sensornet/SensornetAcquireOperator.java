package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAcquireOperator extends SensornetOperatorImpl {

	private static Logger logger 
	= Logger.getLogger(SensornetAcquireOperator.class.getName());
	
	AcquireOperator acqOp;
	
	SensorNetworkSourceMetadata sourceMetadata;
	
	public SensornetAcquireOperator(LogicalOperator op) throws SNEEException,
			SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAcquireOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		acqOp = (AcquireOperator) op;
		this.setNesCTemplateName("acquire");
		//TODO: Separate acquire operator will be need for each source?
		this.sourceMetadata = (SensorNetworkSourceMetadata) 
			this.acqOp.getSources().get(0);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAcquireOperator()");
		}		
	}

	@Override
	public int[] getSourceSites() {
		return this.sourceMetadata.getSourceSites();
	}


}
