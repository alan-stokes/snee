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
import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;

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
	  		   	
//	  		   	String nesCType = attrType.getNesCName();
//	  		   	if (incrAttr.getAggrFunction() == AggregationFunction.SUM) {
//	  		   		nesCType = "uint64_t";
//	  		   	}
	  		   	String nesCType = "float";
	  		   	aggrVariablesBuff.append("\t" + nesCType + " " + attrName + ";\n");       			

	  		   	String baseAttr = incrAttr.getBaseAttribute().getAttributeDisplayName().replace('.','_');
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
				
	  		   	String baseAttr = incrAttr.getBaseAttribute().getAttributeDisplayName().replace('.', '_');
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
					inputAttrName = baseAttr.getAttributeDisplayName().replaceAll("\\.", "_");
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
					String baseAttrName = baseAttr.getAttributeDisplayName().replace('.', '_');
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
				String displayName = attr.getAttributeDisplayName().replace('.', '_');
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String averageVar = displayName+"_avg";
					final String nesCType = "float";
					derivedAggregatesDeclsBuff.append("\t"+nesCType+" "+averageVar+";\n");
				}
				if ((aggrFn == AggregationFunction.STDEV)) {
					String stdevVar = displayName+"_stdev";
					String avgForStdevVar = displayName+"_avg_for_stdev";
					derivedAggregatesDeclsBuff.append("\tfloat "+stdevVar+";\n");
					derivedAggregatesDeclsBuff.append("\tfloat "+avgForStdevVar+";\n");
					derivedAggregatesDeclsBuff.append("\tfloat tmpDiff;\n");
					derivedAggregatesDeclsBuff.append("\tfloat tmpSum;\n");
				}
			}
		}
		return derivedAggregatesDeclsBuff;
    }
    
    public static StringBuffer computeDerivedIncrAggregates(List<AggregationExpression> aggregates, CodeGenTarget target) {
    	final StringBuffer derivedAggregatesBuff = new StringBuffer();
    	
		for (AggregationExpression aggr : aggregates) {
			List<Attribute> attributes = aggr.getRequiredAttributes();
			for (Attribute attr : attributes) {
				String displayName = attr.getAttributeDisplayName().replace('.', '_');
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				if ((aggrFn == AggregationFunction.AVG)) {
					String countVar = displayName+"_count";
					String sumVar = displayName+"_sum";
					String averageVar = displayName+"_avg";
					derivedAggregatesBuff.append("\t\t"+averageVar+
							" = ((float)"+sumVar+" / (float)"+countVar+");\n");
				}
				if ((aggrFn == AggregationFunction.STDEV)) {
					String countVar = displayName+"_count";
					String sumVar = displayName+"_sum";
					String avgForStdevVar = displayName+"_avg_for_stdev";
					String stdevVar = displayName+"_stdev";
					
					derivedAggregatesBuff.append("\t\t" + avgForStdevVar +
							" = ((float)"+sumVar+" / (float)"+ countVar+");\n");
					derivedAggregatesBuff.append("\t\tinHead = inHead2;\n");
					derivedAggregatesBuff.append("\t\tinTail = inTail2;\n");	
					derivedAggregatesBuff.append("\t\ttmpSum = 0.0;\n");
					derivedAggregatesBuff.append("\t\tdo\n\t\t{\n");
					derivedAggregatesBuff.append("\t\t\ttmpDiff = (inQueue[inHead]."+
							displayName+" - "+avgForStdevVar+");\n");
					derivedAggregatesBuff.append("\t\t\ttmpSum += (tmpDiff * tmpDiff);\n\n");
					derivedAggregatesBuff.append("\t\t\tinHead=(inHead+1) % inQueueSize;\n\t\t}\n");
					derivedAggregatesBuff.append("\t\twhile(inHead!=inTail);\n");
					
					if (target == CodeGenTarget.AVRORA_MICA2_T2 || 
							target == CodeGenTarget.AVRORA_MICAZ_T2) {
						derivedAggregatesBuff.append("\t\t"+stdevVar+" = (float)sqrt((double)tmpSum / ((double)"+countVar+" - 1.0));\n");
					} else {
						derivedAggregatesBuff.append("\t\t"+stdevVar+" = sqrtf((float)tmpSum / ((float)"+countVar+" - 1.0));\n");						
					}
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
	
    
    public static StringBuffer generateTuple(
    		List <Attribute> outputAttributes, List<AggregationExpression> aggregates) {
    	final StringBuffer tupleBuffer = new StringBuffer();
		tupleBuffer.append("\t\toutQueue[outTail]." 
				+ "evalEpoch = currentEvalEpoch;\n");
    	
    	int i = 1;
		for (AggregationExpression aggr : aggregates) {
			//works because there is one required attribute per aggregate expression
			List<Attribute> aggrAttributes = aggr.getRequiredAttributes();
			for (Attribute aggrAttribute : aggrAttributes) {
				AggregationFunction aggrFn = aggr.getAggregationFunction();
				String aggrAttrDisplayName = aggrAttribute.
					getAttributeDisplayName().replace('.', '_') +
					"_" + aggrFn.toString();
				
				Attribute outputAttribute = outputAttributes.get(i);
				String outputAttrDisplayName = outputAttribute.
					getAttributeDisplayName().replace('.', '_');


				tupleBuffer.append("\t\toutQueue[outTail]." 
						+ outputAttrDisplayName + " = " + aggrAttrDisplayName + ";\n");	
				
				i++;
			}
    	}
    	
    	return tupleBuffer;
    }
}
