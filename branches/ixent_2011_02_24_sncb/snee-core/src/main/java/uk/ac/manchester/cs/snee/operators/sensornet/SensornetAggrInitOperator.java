package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.jar.Attributes;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationType;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperatorImpl;

public class SensornetAggrInitOperator extends SensornetIncrementalAggregationOperator {

	private static Logger logger 
	= Logger.getLogger(SensornetAggrInitOperator.class.getName());
	
	ArrayList<Attribute> outputAttributes = new ArrayList<Attribute>();
	
	public SensornetAggrInitOperator(LogicalOperator op, CostParameters costParams) throws SNEEException,
			SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAggrInitOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}	
		this.setNesCTemplateName("aggrinit");
		this.setOperatorName("SensornetAGGRInit");
		updateOutputAttributes();
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAggrInitOperator()");
		}
	}

    private void updateOutputAttributes() throws SchemaMetadataException {
		List<AggregationExpression> aggregates = super.getAggregates();
		
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				AttributeType attrType = attr.getType();
				AggregationType aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationType.AVG) || (aggrFn == AggregationType.STDEV)) {
					String newAttrName = schemaName+"_"+AggregationType.COUNT;
					addAttribute(extentName, newAttrName, attrType);
					String newAttrName2 = schemaName+"_"+AggregationType.SUM;
					addAttribute(extentName, newAttrName2, attrType);
				} else {
					String newAttrName = schemaName+"_"+aggrFn;
					addAttribute(extentName, newAttrName, attrType);
				}
			}			
		}		
	}

    private void addAttribute(String extentName, String schemaName, AttributeType type)
    throws SchemaMetadataException {
    	//check for dups
    	for (Attribute oa: this.outputAttributes) {
    		if ((oa.getExtentName().equals(extentName)) && 
    			(oa.getAttributeSchemaName().equals(schemaName))) {
    			return; //duplicate, do not add
    		}
    	}
    	
		Attribute newAttr = new DataAttribute(extentName, schemaName, type);
		this.outputAttributes.add(newAttr); 
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
    
	public List<Attribute> getAttributes() {
		return this.outputAttributes;
	}
	
	public String getTupleAttributesStr(int maxPerLine) 
	throws SchemaMetadataException, TypeMappingException {
		return LogicalOperatorImpl.getTupleAttributesStr(this.outputAttributes, maxPerLine);
	}
    
}
