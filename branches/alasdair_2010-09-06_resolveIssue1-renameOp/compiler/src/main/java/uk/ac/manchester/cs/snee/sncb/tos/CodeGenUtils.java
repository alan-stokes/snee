/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.sncb.tos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.operators.logical.AggregationType;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetIncrementalAggregationOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

/**
 * Utility Class used by the various codeGeneration components.
 * @author Ixent and Christian
 *
 */
public final class CodeGenUtils {

	/** Hides default constructor. */
	private CodeGenUtils() {	
	}
	
    //static Logger logger = Logger.getLogger(CodeGenUtils.class.getName());

    public static HashMap<String, Integer> outputTypeSize = new HashMap<String, Integer>();

    /**
     * An operator instance name takes the following form:
     * 		{opName}Op{opID}Frag{fragID}Site{siteID}P, where
     *  
     * @param opID		the unique identifier of the operator in the query plan operator tree
     * @param siteID	the identifier of the site in the sensor network where this instance is located	
     * @param fragID	the identifier of the query plan fragment which the operator is in
     * @return
     */
    public static String generateOperatorInstanceName(SensornetOperator op,
	final Site site, int tosVersion) {
    	Fragment frag = op.getContainingFragment();
    	if (frag==null)
    		frag = op.getLeftChild().getContainingFragment();
		if (tosVersion == 1) {
		    return op.getNesCTemplateName() + "Op" + op.getID() + "Frag"
			    + frag.getID() + "Site" + site.getID() + "M";
		} else {
		    return op.getNesCTemplateName() + "Op" + op.getID() + "Frag"
			    + frag.getID() + "Site" + site.getID() + "P";
		}
    }

    /**
     * The rules for naming an operator tuple type are as follows:
     * 
     * (1) An operator's default type name is based on the operator's unique identifier, and takes 
     * the form xxxOp{opID}, where xx is the id of the operator in the query plan operator tree.  
     * (2) The exception for (1) is when the operator is a deliver, an exchange producer, or the child 
     * of either a deliver or exchange producer.  In this case it takes the form xxxFrag{fragID}, where 
     * fragID is the id of the fragment that the operator belongs to.
     * (3) The exception for (2) is in the case of merge fragments (previously referred to as 
     * "recursive fragments"), which must have the same output tuple type as their input tuple type.  
     * Therefore, the default type name for a merge fragment's output type is its child fragment output 
     * type.
     * 
     * @param op	The operator for which a tuple type is to be generated
     * @return
     */
    private static String generateTypeName(final String prefix,
	    final Fragment frag) {
	if (frag.isRecursive()) {
	    return prefix + "Frag" + frag.getChildFragments().get(0).getID();
	} else {
	    return prefix + "Frag" + frag.getID();
	}
    }

    private static String generateTypeName(final String prefix,
	    final SensornetOperator op) {
	if (op.isFragmentRoot()) {
	    return generateTypeName(prefix, op.getContainingFragment());
	} else if (op instanceof SensornetExchangeOperator) {
	    return generateTypeName(prefix, ((SensornetOperator)op.getInput(0))
		    .getContainingFragment());
	} else {
	    return prefix + "Op" + op.getID();
	}
    }

    public static String generateOutputTupleType(final Fragment frag) {
	return generateTypeName("Tuple", frag);
    }

    public static String generateOutputTupleType(final SensornetOperator op) {
	return generateTypeName("Tuple", op);
    }

    public static String generateOutputTuplePtrType(final Fragment frag) {
	return generateOutputTupleType(frag) + "Ptr";
    }

    public static String generateOutputTuplePtrType(final SensornetOperator op) {
	return generateOutputTupleType(op) + "Ptr";
    }

    public static String generateMessageType(final Fragment frag) {
	return generateTypeName("Message", frag);
    }

    public static String generateMessageType(final SensornetOperator op) {
	return generateTypeName("Message", op);
    }

    public static String generateMessagePtrType(final SensornetOperator op) {
	return generateMessageType(op) + "Ptr";
    }

    public static String generateMessagePtrType(final Fragment frag) {
	return generateMessageType(frag) + "Ptr";
    }

    public static String generateGetTuplesInterfaceInstanceName(
	    final SensornetOperator op) {
	return generateTypeName(TinyOSGenerator.INTERFACE_GET_TUPLES, op);
    }

    public static String generateGetTuplesInterfaceInstanceName(
	    final Fragment frag) {
	return generateTypeName(TinyOSGenerator.INTERFACE_GET_TUPLES, frag);
    }

    public static String generateGetTuplesInterfaceInstanceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destSiteID, final String currentSiteID) {
	return generateTypeName(TinyOSGenerator.INTERFACE_GET_TUPLES, sourceFrag)
		+ "_F"
		+ destFrag.getID()
		+ "n"
		+ destSiteID
		+ "_n"
		+ currentSiteID;
    }

    public static String generatePutTuplesInterfaceInstanceName(
	    final Fragment frag) {
	return generateTypeName(TinyOSGenerator.INTERFACE_PUT_TUPLES, frag);
    }

    public static String generatePutTuplesInterfaceInstanceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destSiteID, final String currentSiteID) {
	return generateTypeName(TinyOSGenerator.INTERFACE_PUT_TUPLES, sourceFrag)
		+ "_F"
		+ destFrag.getID()
		+ "n"
		+ destSiteID
		+ "_n"
		+ currentSiteID;
    }

    public static String generateUserSendInterfaceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destNodeID) {
	return generateTypeName(TinyOSGenerator.INTERFACE_SEND, sourceFrag)
		+ "_F" + destFrag.getID() + "n" + destNodeID;
    }

    public static String generateUserAsDoTaskName(final String prefix,
	    final String sendingNodeID, final Fragment sourceFrag,
	    final String destNodeID, final Fragment destFrag) {
	return TinyOSGenerator.INTERFACE_DO_TASK + prefix + "_F"
		+ sourceFrag.getID() + "n" + sendingNodeID + "F"
		+ destFrag.getID() + "n" + destNodeID;
    }

    public static String generateUserAsDoTaskName(final Fragment frag,
	    final Site currentSite) {
	return TinyOSGenerator.INTERFACE_DO_TASK + "Frag" + frag.getID() + "n"
		+ currentSite.getID();
    }

    public static String generateUserAsDoTaskName(final Fragment frag,
	    final String currentSite) {
	return TinyOSGenerator.INTERFACE_DO_TASK + "Frag" + frag.getID() + "n"
		+ currentSite;
    }

    public static String generateProviderSendInterfaceName(String key) {
    	final String activeMessageID = ActiveMessageIDGenerator
		.getActiveMessageID(key);
    	return "SendMsg[" + activeMessageID + "]";
    }
    
    public static String generateProviderSendInterfaceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destNodeID, final String sendingNodeID) {
	final String activeMessageID = ActiveMessageIDGenerator
		.getActiveMessageID(sourceFrag.getID(), destFrag.getID(),
			destNodeID, sendingNodeID);
	return "SendMsg[" + activeMessageID + "]";
    }

    public static String generateUserReceiveInterfaceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destNodeID) {
	return generateTypeName(TinyOSGenerator.INTERFACE_RECEIVE, sourceFrag)
		+ "_F" + destFrag.getID() + "n" + destNodeID;
    }

    public static String generateProviderReceiveInterfaceName(
	    final Fragment sourceFrag, final Fragment destFrag,
	    final String destNodeID, final String sendingNodeID) {
	final String activeMessageID = ActiveMessageIDGenerator
		.getActiveMessageID(sourceFrag.getID(), destFrag.getID(),
			destNodeID, sendingNodeID);
	return "ReceiveMsg[" + activeMessageID + "]";
    }

    public static StringBuffer getPartialAggrVariables(
    final List<Attribute> attributes) 
    throws SchemaMetadataException, TypeMappingException {
       	final StringBuffer aggrVariablesBuff = new StringBuffer();
       	for (int i = 1; i < attributes.size(); i++) {
   			String attrName 
   				= CodeGenUtils.getNescAttrName(attributes.get(i));
  		   	final AttributeType attrType = attributes.get(i).getType();
  		   	final String nesCType = attrType.getNesCName();
  		   	aggrVariablesBuff.append("\t" + nesCType + " " + attrName + ";\n");
		}	
	    return aggrVariablesBuff;
    } 
    	
    	/*	
	final Iterator<String> attrIter = op.getTupleAttributes().keySet()
		.iterator();
	while (attrIter.hasNext()) {
	    final String attr = attrIter.next();
	    if (attr.startsWith("COUNT_FOR_")) {
		replacements.put("__AGGR_COUNT_VARIABLE__", attr.replace(".",
			"_"));
	    } else if (attr.startsWith("SUM_FOR_")) {
		replacements.put("__AGGR_SUM_VARIABLE__", attr
			.replace(".", "_"));
	    }
	}*/
 

    /*
    private static String generatePredicateArgument(final Operator op,
	    final Predicate p, final int argNum) {
	Variable arg;
	if (argNum == 1) {
	    arg = p.getArgument1();
	} else {
	    arg = p.getArgument2();
	}

	String argStr = arg.toString();

	if (arg instanceof OldAttribute) {
	    if (op instanceof JoinOperator) {
		final JoinOperator joinOp = (JoinOperator) op;
		if (joinOp.comesFromRightChild(argStr)) {
		    argStr = argStr.replaceAll("\\.", "_");
		    argStr = "rightInQueue[tmpRightInHead]." + argStr;
		} else {
		    argStr = argStr.replaceAll("\\.", "_");
		    argStr = "leftInQueue[leftInHead]." + argStr;
		}
	    } else if (op instanceof AcquireOperator) {
		argStr = argStr.replaceAll("\\.", "_");
		argStr = "outQueue[outTail]." + argStr;
	    } else {
		argStr = argStr.replaceAll("\\.", "_");
		argStr = "inQueue[inHead]." + argStr;
	    }
	}
	
	return argStr;
    }
	*/
    /** 
    public static String generatePredicatesString(
	    final Collection<Predicate> predicates, final Operator op) {
	final StringBuffer predicatesBuff = new StringBuffer();
	boolean first = true;

	predicatesBuff.append("(");
	final Iterator<Predicate> predIter = predicates.iterator();
	while (predIter.hasNext()) {
	    final Predicate p = predIter.next();
	    if (first) {
		first = false;
	    } else {
		predicatesBuff.append(" && ");
	    }

	    final String arg1 = generatePredicateArgument(op, p, 1);
	    final String arg2 = generatePredicateArgument(op, p, 2);

	    //TODO: insert any SNQL->nesC operator mappings here
	    String opStr = p.getOperator();
	    if (opStr.equals("=")) {
		opStr = "==";
	    }

	    predicatesBuff.append("(" + arg1 + " " + opStr + " " + arg2 + ")");
	}

	//if no predicates in collection then evaluate to true
	if (first) {
	    predicatesBuff.append(" TRUE ");
	}
	predicatesBuff.append(")");

	return predicatesBuff.toString();
    }
	*/
    

    /**
     * Generates the tuple construction of all operators 
     * except acquire and joins.
     * 
     * @param op Operator for which tuple construction is being done.
     * @param ignore Debg parameter which drops attributes with "ignore" in it.
     * @return NesC tuple construction code.
     * @throws CodeGenerationException 
     */
    public static StringBuffer generateTupleConstruction(
    	final SensornetOperator op, boolean ignore) throws CodeGenerationException {
    	final StringBuffer tupleConstructionBuff = new StringBuffer();
    	final List <Attribute> attributes = 
    		op.getLogicalOperator().getAttributes();
    	final List <Attribute> input = 
    		((SensornetOperator)op.getInput(0)).getLogicalOperator().getAttributes();
    	final List <Expression> expressions = 
    		op.getLogicalOperator().getExpressions();

    	for (int i = 0; i < attributes.size(); i++) {
				String attrName 
					= CodeGenUtils.getNescAttrName(attributes.get(i));
				String expressionText;
				expressionText = CodeGenUtils.getNescText(expressions.get(i), 
					"inQueue[inHead].", null, input, null);
				if (ignore && attrName.contains("ignore")) {
					tupleConstructionBuff.append("\t\t\t\t\t//SKIPPING outQueue[outTail]."
	    				+ attrName + "=" + expressionText + ";\n");
				} else {	
					tupleConstructionBuff.append("\t\t\t\t\toutQueue[outTail]."
		    				+ attrName + "=" + expressionText + ";\n");
				}
			}		
			return tupleConstructionBuff;
    	}


    /**
     * Generates the instructions to increment the aggregates partial results.
     * Used by the first of the three stages of aggregation.  
     * @param op The operator representing the first stage of aggregation.
     * @return The Nesc code. 
     * @throws CodeGenerationException 
     */
    public static StringBuffer generateIncrementAggregates(
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
				AggregationType type = aggr.getAggregationType();
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
			AggregationType type = aggr.getAggregationType();
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
     * Generates the NesC to reset the variable 
     * which hold partial aggregate results back to zero. 
     * @param attributes The partial results variables.
     * @param op The operator code is being generated for.
     * @return The NesC code.
     */
    public static StringBuffer generateSetAggregatesToZero(
    		final List<Attribute> attributes,
    		final SensornetIncrementalAggregationOperator op) {
    	final StringBuffer incrementAggregatesBuff = new StringBuffer();
    	final List <AggregationExpression> aggregations 
    		= op.getAggregates();
		for (int i = 1; i < attributes.size(); i++) {
			Attribute attribute = attributes.get(i);
			String attrName 
				= CodeGenUtils.getNescAttrName(attribute);
			AggregationExpression aggr = aggregations.get(i);
			AggregationType type = aggr.getAggregationType();
			if ((type == AggregationType.AVG) 
					|| (type == AggregationType.SUM)
					|| (type == AggregationType.COUNT)) {
				incrementAggregatesBuff.append("\t\t\t\t\t"
						+ attrName + " = 0;\n");
			} else {
				//Min and max to do.
				throw new AssertionError("Code not finished " + type);
			}		
		}		
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
    public static StringBuffer generateTupleFromAggregates(
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

    /**
     * Generates the nesc call for this expression.
     * 
     * @param expression Expression to get nesc call for.
     * @param leftHead 
     *       Text to be place before is attribute comes from the left.
     * @param rightHead 
     *       Text to be place before is attribute comes from the right.
     * @param input List of attributes coming from the first child.
     * @param rightAttributes List of attribues coming from a second child.
     *      Only used with joins.
     * @return The text to be used in the Nesc code.
     * @throws CodeGenerationException Error if attribute not found.
     * See also AcquireComponent.getNescText
     */
	public static String getNescText(final Expression expression, 
			final String leftHead, final String rightHead, 
			final List<Attribute> input, 
			final List<Attribute>rightAttributes) 
			throws CodeGenerationException {
		if (expression instanceof EvalTimeAttribute) {
			return "currentEvalEpoch";
		}
		if (expression instanceof Attribute) {
			Attribute attr = (Attribute) expression;
			if (input.contains(attr)) {
				return (leftHead + CodeGenUtils.getNescAttrName(attr));		
			} else { 
				if (rightAttributes != null) { 
					if (rightAttributes.contains(attr)) {
						return (rightHead + CodeGenUtils.getNescAttrName(attr));
					} else {
						throw new CodeGenerationException("Neither left "
							+ "(" + input + ")"
							+ "nor right (" + rightAttributes + ")"
							+ "inputs contained " + attr);
					}
				} else {
					throw new CodeGenerationException("Input "
						+ "(" + input + ")"
						+ "did not contain " + attr);
				}
			}
		}	
		if (expression instanceof MultiExpression) {
			MultiExpression multi = (MultiExpression) expression;
			Expression[] expressions = multi.getExpressions(); 
			String output = "(" + getNescText(expressions[0],
				leftHead, rightHead, input, rightAttributes);
			for (int i = 1; i < expressions.length; i++) {
				output = output + multi.getMultiType().getNesC() 
					+ getNescText(expressions[i],
					leftHead, rightHead, input, rightAttributes);
			}
			return output + ")";
		}
		if (expression instanceof NoPredicate) {
			return "TRUE";
		}
		if (expression instanceof IntLiteral) {
			return expression.toString();
		}
		if (expression instanceof AggregationExpression) {
			AggregationExpression aggregate 
				= (AggregationExpression) expression; 
			if (aggregate.getAggregationType() 
					== AggregationType.AVG) {
				return "(" + Constants.PARTIAL_LOCALNAME + "_"
					+ Constants.AVG_PARTIAL_HEAD 
					+ aggregate.getShortName() + "/ "
					+ Constants.PARTIAL_LOCALNAME + "_count )";
			}	
			return Constants.PARTIAL_LOCALNAME + "_" + aggregate.getShortName();
		}
		
		throw new CodeGenerationException("Missing code. Expression "
			+ expression);	
	}
	
	/**
	 * Generates the name Nesc should use for this attribute. 
	 * @param attr The Attribute a name should be generated for.
	 * @return The String to use in nesc records.
	 */
	public static String getNescAttrName(final Attribute attr) {
		if (attr instanceof EvalTimeAttribute) {
			return "evalEpoch";
		}
		if (attr instanceof TimeAttribute) {
			return attr.getLocalName() + ("_epoch");
		}
//TODO: Localtime
//		if (attr instanceof LocalTimeAttribute) {
//			return Constants.LOCAL_TIME;
//		}
		return attr.getLocalName() + "_" + attr.getAttributeName(); 
	}

	/**
	 * Generates the name Deliver should use for this attribute. 
	 * @param attr The Attribute a name should be generated for.
	 * @return The String to used by deliver.
	 */
	public static String getDeliverName(final Attribute attr) {
		if (attr instanceof EvalTimeAttribute) {
			return "evalEpoch";
		}
		if (attr instanceof TimeAttribute) {
			return attr.getLocalName() + ("Epoch");
		}
		return attr.getAttributeName(); 
	}


}
