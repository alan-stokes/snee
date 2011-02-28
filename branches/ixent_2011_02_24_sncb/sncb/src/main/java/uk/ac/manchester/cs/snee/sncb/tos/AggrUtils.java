package uk.ac.manchester.cs.snee.sncb.tos;

import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IncrementalAggregationAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationType;
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
				incrementAggregatesBuff.append("\t\t\t\t\t"
						+ attrName + " = 0;\n");
			}		
		}
		incrementAggregatesBuff.append("\t\t\t\t\tbool tuplesReceived = FALSE;\n");
		return incrementAggregatesBuff;
    } 
    
    /**
     * Generates the instructions to increment the aggregates partial results.
     * Used by the first of the three stages of aggregation.  
     * @param op The operator representing the first stage of aggregation.
     * @return The Nesc code. 
     * @throws CodeGenerationException 
     */
    public static StringBuffer generateVarsIncrement(
    	final SensornetIncrementalAggregationOperator op) throws CodeGenerationException {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();
    	final List <Attribute> attributes = op.getAttributes();
    	final List <Attribute> input = op.getLeftChild().getAttributes();
    	final List <AggregationExpression> aggregations 
    		= op.getAggregates();

    	//skip Attribute 0 which is EvalTime
			for (int i = 1; i < attributes.size(); i++) {
				Attribute attribute = attributes.get(i);
				String attrName 
					= CodeGenUtils.getNescAttrName(attribute);
				AggregationExpression aggr = aggregations.get(i);
				AggregationType type = aggr.getAggregationFunction();
				Expression expression = aggr.getExpression();
				if ((type == AggregationType.AVG) 
						|| (type == AggregationType.SUM)) {
					String expressionText = CodeGenUtils.getNescText(
							expression, "inQueue[inHead].", null, input, null);
					incrementAggregatesBuff.append("\t\t\t\t\t" 
							+ attrName + " += " + expressionText + ";\n");
				} else if ((type == AggregationType.COUNT)) {
					incrementAggregatesBuff.append("\t\t\t\t\t"
							+ attrName + "++;\n");
				} else {
					//Min and max to do.
					throw new AssertionError("Code not finished " + type);
				}		
			}		
			return incrementAggregatesBuff;
    } 

    /**
     * Generates the instructions to increment the aggregates partial results.
     * Used by the second and third of the three stages of aggregation.  
     * @param attributes The partial results variables.
     * @param op The operator representing the first stage of aggregation.
     * @return The Nesc code. Including specific debug statements.
     */
    public static StringBuffer generateIncrementAggregates(
    		final List<Attribute> attributes,
    		final SensornetIncrementalAggregationOperator op) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();
    	final StringBuffer dbgIn1Buff 
    		= new StringBuffer("\t\t\t\t\tdbg(DBG_USR1,\"");
    	final StringBuffer dbgIn2Buff = new StringBuffer("\n,");
    	final StringBuffer dbgOut1Buff 
    		= new StringBuffer("\t\t\t\t\tdbg(DBG_USR1,\"");
    	final StringBuffer dbgOut2Buff = new StringBuffer("\n,");
    	final List<AggregationExpression> aggregations 
    		= op.getAggregates();
		//skip Attribute 0 which is EvalTime
		for (int i = 1; i < attributes.size(); i++) {
			Attribute attribute = attributes.get(i);
			String attrName 
				= CodeGenUtils.getNescAttrName(attribute);
			AggregationExpression aggr = aggregations.get(i);
			AggregationType type = aggr.getAggregationFunction();
			if ((type == AggregationType.AVG) 
					|| (type == AggregationType.COUNT)
					|| (type == AggregationType.SUM)) {
				incrementAggregatesBuff.append("\t\t\t\t\t" 
						+ attrName + " += inQueue[inHead]." 
						+ attrName + ";\n");
//		    	if (Settings.NESC_MAX_DEBUG_STATEMENTS_IN_AGGREGATES) {
//		    		dbgIn1Buff.append("inQueue[inHead]." + attrName + "= %d ");
//		    		dbgIn2Buff.append("inQueue[inHead]." + attrName + ",");
//		    		dbgOut1Buff.append(attrName + "= %d ");
//		    		dbgOut2Buff.append(attrName + ",");
//		    	}
			} else {
				//Min and max to do.
				throw new AssertionError("Code not finished " + type);
			}		
		}		
//    	if (Settings.NESC_MAX_DEBUG_STATEMENTS_IN_AGGREGATES) {
//    		incrementAggregatesBuff.append(dbgIn1Buff.toString() 
//				+ "inHead = %d\\n\"" + dbgIn2Buff.toString() + "inHead);\n");
//    		incrementAggregatesBuff.append(dbgOut1Buff);
//    		incrementAggregatesBuff.append("inHead = %d\\n\"");
//    		incrementAggregatesBuff.append(dbgOut2Buff);
//    		incrementAggregatesBuff.append("inHead);\n");
//    	}
		return incrementAggregatesBuff;
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
    	//skip 0 element which is evaltime 
		for (int i = 1; i < attributes.size(); i++) {
			String attrName 
				= CodeGenUtils.getNescAttrName(attributes.get(i));
			incrementAggregatesBuff.append("\t\t\t\t\toutQueue[outTail]." 
					+ attrName + " = " + attrName + ";\n");
		}		
		return incrementAggregatesBuff;
    } 
	
}
