package uk.ac.manchester.cs.snee.sncb.tos;

import java.util.HashSet;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IncrementalAggregationAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;

public class AggrUtils {

    public static StringBuffer generateVarDecls(final List<Attribute> attributes) 
    throws SchemaMetadataException, TypeMappingException {
       	final StringBuffer aggrVariablesBuff = new StringBuffer();
       	HashSet<String> baseAttributes = new HashSet<String>();
       	
       	for (Attribute attr: attributes) {
       		
       		if (attr instanceof IncrementalAggregationAttribute) {
       			IncrementalAggregationAttribute incrAttr = 
       				(IncrementalAggregationAttribute)attr;
       			String attrName = 
       				CodeGenUtils.getNescAttrName(incrAttr);
	  		   	final AttributeType attrType = incrAttr.getType();	  		   	
	  		   	
	  		   	String nesCType = attrType.getNesCName();
	  		   	if (incrAttr.getAggrFunction() == AggregationFunction.SUM) {
	  		   		nesCType = "uint64_t";
	  		   	}
	  		   	aggrVariablesBuff.append("\t" + nesCType + " " + attrName + ";\n");       			

	  		   	String baseAttr = incrAttr.getBaseAttribute().getAttributeSchemaName();
	       		baseAttributes.add(baseAttr);
       		}
		}	
       	
       	for (String baseAttr: baseAttributes) {
          	aggrVariablesBuff.append("\tbool "+baseAttr+"_tuplesReceived;\n");      		
       	}
 
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
       	HashSet<String> baseAttributes = new HashSet<String>();

		for (Attribute attr : attributes) {
			
       		if (attr instanceof IncrementalAggregationAttribute) {
       			IncrementalAggregationAttribute incrAttr = 
       				(IncrementalAggregationAttribute)attr;		
				String attrName 
					= CodeGenUtils.getNescAttrName(incrAttr);
				incrementAggregatesBuff.append("\t\t\t"
						+ attrName + " = 0;\n");
				
	  		   	String baseAttr = incrAttr.getBaseAttribute().getAttributeSchemaName();
	       		baseAttributes.add(baseAttr);
			}		
		}
		
       	for (String baseAttr: baseAttributes) {
       		incrementAggregatesBuff.append("\t\t\t"+baseAttr+"_tuplesReceived = FALSE;\n");
       	}
       	
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
				Attribute baseAttr = incrAttr.getBaseAttribute();
				String inputAttrName = attrName;
				if (initFlag) {
					inputAttrName = baseAttr.getExtentName()+"_"+baseAttr.getAttributeSchemaName();
				}
				if (aggrFn == AggregationFunction.SUM) {
					incrementAggregatesBuff.append("\t\t\t\t" 
							+ attrName + " = ("+attrName+" + inQueue[inHead]." 
							+ inputAttrName + ");\n");
				} 
				if (aggrFn == AggregationFunction.COUNT) {
					if (initFlag) {
						incrementAggregatesBuff.append("\t\t\t\t" 
								+ attrName + "++;\n");		
					} else {
						incrementAggregatesBuff.append("\t\t\t\t" 
								+ attrName + " += inQueue[inHead]." 
								+ inputAttrName + ";\n");
					}
				}

				String comp = "<";
				if (aggrFn==AggregationFunction.MAX) {
					comp = ">";
				}
				if (aggrFn == AggregationFunction.MIN || aggrFn == 
					AggregationFunction.MAX) {
					String baseAttrName = baseAttr.getAttributeSchemaName();
					incrementAggregatesBuff.append("\t\t\t\tif " +
							"(("+baseAttrName+"_tuplesReceived==FALSE) || (inQueue[inHead]."+
							inputAttrName+" "+comp+" " + attrName + "))\n");
					incrementAggregatesBuff.append("\t\t\t\t{\n\t\t\t\t\t");
					incrementAggregatesBuff.append(attrName + " = inQueue[inHead]." 
							+ inputAttrName + ";\n");
					incrementAggregatesBuff.append("\t\t\t\t\t"+baseAttrName+"_tuplesReceived=TRUE;\n");
					incrementAggregatesBuff.append("\t\t\t\t}\n");
				}		
			}
		}
		return incrementAggregatesBuff;
    } 


    public static StringBuffer generateDerivedIncrAggregatesDecls(List<AggregationExpression> aggregates) {
    	final StringBuffer derivedAggregatesDeclsBuff = new StringBuffer();
    	
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String averageVar = extentName+"_"+schemaName+"_avg";
					final String nesCType = "float";
					derivedAggregatesDeclsBuff.append("\t"+nesCType+" "+averageVar+";\n");
				}
				if ((aggrFn == AggregationFunction.STDEV)) {
					String stdevVar = extentName+"_"+schemaName+"_stdev";
					String avgForStdevVar = extentName+"_"+schemaName+"_avg_for_stdev";
					derivedAggregatesDeclsBuff.append("\tfloat "+stdevVar+";\n");
					derivedAggregatesDeclsBuff.append("\tfloat "+avgForStdevVar+";\n");
					derivedAggregatesDeclsBuff.append("\tfloat tmpDiff;\n");
					derivedAggregatesDeclsBuff.append("\tfloat tmpSum;\n");
				}
			}
		}
		return derivedAggregatesDeclsBuff;
    }
    
    public static StringBuffer computeDerivedIncrAggregates(List<AggregationExpression> aggregates) {
    	final StringBuffer derivedAggregatesBuff = new StringBuffer();
    	
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String extentName = attr.getExtentName();
				String schemaName = attr.getAttributeSchemaName();
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String countVar = extentName+"_"+schemaName+"_count";
					String sumVar = extentName+"_"+schemaName+"_sum";
					String averageVar = extentName+"_"+schemaName+"_avg";
					derivedAggregatesBuff.append("\t\t"+averageVar+
							" = ((float)"+sumVar+" / (float)"+countVar+");\n");
				}
				if ((aggrFn == AggregationFunction.STDEV)) {
					String countVar = extentName+"_"+schemaName+"_count";
					String sumVar = extentName+"_"+schemaName+"_sum";
					String avgForStdevVar = extentName+"_"+schemaName+"_avg_for_stdev";
					String stdevVar = extentName+"_"+schemaName+"_stdev";
					
					derivedAggregatesBuff.append("\t\t" + avgForStdevVar +
							" = ((float)"+sumVar+" / (float)"+ countVar+");\n");
					derivedAggregatesBuff.append("\t\tinHead = inHead2;\n");
					derivedAggregatesBuff.append("\t\tinTail = inTail2;\n");	
					derivedAggregatesBuff.append("\t\ttmpSum = 0.0;\n");
					derivedAggregatesBuff.append("\t\tdo\n\t\t{\n");
					derivedAggregatesBuff.append("\t\t\ttmpDiff = (inQueue[inHead]."+
							extentName+"_"+schemaName+" - "+avgForStdevVar+");\n");
					derivedAggregatesBuff.append("\t\t\ttmpSum += (tmpDiff * tmpDiff);\n\n");
					derivedAggregatesBuff.append("\t\t\tinHead=(inHead+1) % inQueueSize;\n\t\t}\n");
					derivedAggregatesBuff.append("\t\twhile(inHead!=inTail);\n");
					derivedAggregatesBuff.append("\t\t"+stdevVar+" = sqrt((float)tmpSum / ((float)"+countVar+" - 1.0));\n");
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
    		List <Attribute> attributes) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();
    	
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
