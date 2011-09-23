package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IncrementalAggregationAttribute;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAggrInitOperator extends SensornetIncrementalAggregationOperator {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -3495147217949546780L;

  private static final Logger logger = Logger.getLogger(SensornetAggrInitOperator.class.getName());
	
	private ArrayList<Attribute> outputAttributes = new ArrayList<Attribute>();
	
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
		this.outputAttributes = getIncrementalAggregationAttributes(this.aggrOp);
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAggrInitOperator()");
		}
	}

    public static ArrayList<Attribute> getIncrementalAggregationAttributes(AggregationOperator aggrOp) 
    throws SchemaMetadataException {
		ArrayList<Attribute> result = new ArrayList<Attribute>();
    	List<AggregationExpression> aggregates = aggrOp.getAggregates();
		
		Attribute evalTimeAttr = new EvalTimeAttribute();
		result.add(evalTimeAttr); 
		
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				String displayName = attr.getAttributeDisplayName();
				
				AttributeType attrType = attr.getType();
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG) || (aggrFn == AggregationFunction.STDEV)) {
					String newAttrName = schemaName+"_"+AggregationFunction.COUNT;
					String newAttrDisplayName = displayName+"_"+AggregationFunction.COUNT;
					addAttribute(extentName, newAttrName, newAttrDisplayName, attrType, attr, AggregationFunction.COUNT, result);
					String newAttrName2 = schemaName+"_"+AggregationFunction.SUM;
					String newAttrDisplayName2 = displayName+"_"+AggregationFunction.SUM;
					addAttribute(extentName, newAttrName2, newAttrDisplayName2, attrType, attr, AggregationFunction.SUM, result);
				} else {
					String newAttrName = schemaName+"_"+aggrFn;
					String newAttrDisplayName = displayName+"_"+aggrFn;
					addAttribute(extentName, newAttrName, newAttrDisplayName, attrType, attr, aggrFn, result);
				}
			}			
		}
		return result;
	}

    private static void addAttribute(String extentName, String schemaName, String displayName, AttributeType type,
    		Attribute baseAttribute, AggregationFunction aggrFunction, List<Attribute> result)
    throws SchemaMetadataException {
    	//check for dups
    	for (Attribute a: result) {
    		if ((a.getExtentName().equals(extentName)) && 
    			(a.getAttributeSchemaName().equals(schemaName) &&
    			(a.getAttributeDisplayName().equals(displayName)))) {
    			return; //duplicate, do not add
    		}
    	}
    	
		Attribute newAttr = new IncrementalAggregationAttribute(extentName, schemaName, 
				type, (DataAttribute) baseAttribute, aggrFunction);
		newAttr.setAttributeDisplayName(displayName);
		result.add(newAttr); 
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
    
    public boolean isAttributeSensitive() {
      return false;
    }
    
    public boolean isRecursive() {
      return false;
    }
    
	//not delegated in this case
	public List<Attribute> getAttributes() {
		return this.outputAttributes;
	}
	
}
