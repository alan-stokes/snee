package uk.ac.manchester.cs.snee.sncb.tos;

import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IncrementalAggregationAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetIncrementalAggregationOperator;

public class AggrUtils {

    public static StringBuffer generateVarDecls(final List<Attribute> attributes) 
    throws SchemaMetadataException, TypeMappingException {
       	final StringBuffer aggrVariablesBuff = new StringBuffer();
       	for (Attribute attr: attributes) {
       		
       		if (attr instanceof IncrementalAggregationAttribute) {
       			IncrementalAggregationAttribute incrAttr = 
       				(IncrementalAggregationAttribute)attr;
       			String attrName = 
       				CodeGenUtils.getNescAttrName(incrAttr);
	  		   	final AttributeType attrType = incrAttr.getType();
	  		   	final String nesCType = attrType.getNesCName();
	  		   	aggrVariablesBuff.append("\t" + nesCType + " " + attrName + ";\n");       			
       		}
		}	
       	aggrVariablesBuff.append("\tbool tuplesReceived;\n");
	    return aggrVariablesBuff;
    } 
    	    
	
    /**
     * Generates the NesC to reset the variable 
     * which hold partial aggregate results back to zero. 
     * @param attributes The partial results variables.
     * @param op The operator code is being generated for.
     * @return The NesC code.
     */
    public static StringBuffer generateVarsInit(
    		final List<Attribute> attributes) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();

		for (Attribute attr : attributes) {
			
       		if (attr instanceof IncrementalAggregationAttribute) {
       			IncrementalAggregationAttribute incrAttr = 
       				(IncrementalAggregationAttribute)attr;		
				String attrName 
					= CodeGenUtils.getNescAttrName(incrAttr);
				incrementAggregatesBuff.append("\t\t\t"
						+ attrName + " = 0;\n");
			}		
		}
		incrementAggregatesBuff.append("\t\t\ttuplesReceived = FALSE;\n");
		return incrementAggregatesBuff;
    } 
    

    /**
     * Generates the instructions to increment the aggregates partial results.
     * @param attributes The partial results variables.
     * @return The Nesc code. 
     */
    public static StringBuffer generateIncrementAggregates(
    		final List<Attribute> attributes, boolean initFlag) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();

		for (Attribute attr : attributes) {

       		if (attr instanceof IncrementalAggregationAttribute) {
       			IncrementalAggregationAttribute incrAttr = 
       				(IncrementalAggregationAttribute)attr;		
				String attrName 
					= CodeGenUtils.getNescAttrName(incrAttr);
				AggregationFunction aggrFn = incrAttr.getAggrFunction();
				
				//init uses base attr name as input (e.g., light)
				//merge/eval use incremental attr name as input (e.g., light_sum)
				String inputAttrName = attrName;
				if (initFlag) {
					Attribute baseAttr = incrAttr.getBaseAttribute();
					inputAttrName = baseAttr.getExtentName()+"_"+baseAttr.getAttributeSchemaName();
				}
				
				if ((aggrFn == AggregationFunction.COUNT)
						|| (aggrFn == AggregationFunction.SUM)) {
					incrementAggregatesBuff.append("\t\t\t\t" 
							+ attrName + " += inQueue[inHead]." 
							+ inputAttrName + ";\n");
				} 
				
				String comp = "<";
				if (aggrFn==AggregationFunction.MAX) {
					comp = ">";
				}
				if (aggrFn == AggregationFunction.MIN || aggrFn == 
					AggregationFunction.MAX) {
					incrementAggregatesBuff.append("\t\t\t\tif " +
							"((tuplesReceived==FALSE) || (inQueue[inHead]."+
							inputAttrName+" "+comp+" " + attrName + "))\n");
					incrementAggregatesBuff.append("\t\t\t\t{\n\t\t\t\t\t");
					incrementAggregatesBuff.append(attrName + " = inQueue[inHead]." 
							+ inputAttrName + ";\n");
					incrementAggregatesBuff.append("\t\t\t\t\ttuplesReceived=TRUE;\n");
					incrementAggregatesBuff.append("\t\t\t\t}\n");
				}		
			}
		}
		return incrementAggregatesBuff;
    } 


    public static StringBuffer generateDerivedIncrAggregatesDecls(SensornetAggrEvalOperator op) {
    	final StringBuffer derivedAggregatesDeclsBuff = new StringBuffer();
    	
		List<AggregationExpression> aggregates = op.getAggregates();
		
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				AttributeType attrType = attr.getType();
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String averageVar = extentName+"_"+schemaName+"_avg";
					final String nesCType = attrType.getNesCName();
					derivedAggregatesDeclsBuff.append("\t"+nesCType+" "+averageVar+";\n");
				}
			}
		}
		return derivedAggregatesDeclsBuff;
    }
    
    public static StringBuffer computeDerivedIncrAggregates(SensornetAggrEvalOperator op) {
    	final StringBuffer derivedAggregatesBuff = new StringBuffer();
    	
		List<AggregationExpression> aggregates = op.getAggregates();
		
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				AttributeType attrType = attr.getType();
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String countVar = extentName+"_"+schemaName+"_count";
					String sumVar = extentName+"_"+schemaName+"_sum";
					String averageVar = extentName+"_"+schemaName+"_avg";
					final String nesCType = attrType.getNesCName();
					derivedAggregatesBuff.append("\t\t"+averageVar+
							" = "+sumVar+" / "+countVar+";\n");
				}
			}
		}
		return derivedAggregatesBuff;
    }
    
    /**
     * Generates the tuple construction for partial aggregation operators. 
     * Used by the first two stages.
     * 
     * @param op Operator for which tuple increment aggregation is being done.
     * 
     * @return NesC tuple construction code.
     */
    public static StringBuffer generateTuple(
    	final SensornetIncrementalAggregationOperator op) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();
    	final List <Attribute> attributes = op.getAttributes();
    	
		for (Attribute attr : attributes) {
	
			if (attr instanceof EvalTimeAttribute) {
				incrementAggregatesBuff.append("\t\toutQueue[outTail]." 
						+ "evalEpoch = currentEvalEpoch;\n");
			}
			else
			{
				String attrName 
				= CodeGenUtils.getNescAttrName(attr);
				incrementAggregatesBuff.append("\t\toutQueue[outTail]." 
						+ attrName + " = " + attrName + ";\n");					
			}
			

		}		
		return incrementAggregatesBuff;
    } 
	
}
