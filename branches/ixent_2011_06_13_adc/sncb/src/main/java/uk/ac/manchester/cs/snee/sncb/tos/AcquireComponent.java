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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IDAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

/**
 * Build the Nesc Component for an Acquire operator.
 * @author Ixent, Christian
 *
 */
public class AcquireComponent extends NesCComponent {

	/** Operator class code is being build for. */
    private SensornetAcquireOperator op;

    /** Plan this Operator is in. */
    private SensorNetworkQueryPlan plan;

    public AcquireComponent(final SensornetAcquireOperator op, final SensorNetworkQueryPlan plan,
	    final NesCConfiguration fragConfig,
	    boolean tossimFlag, boolean debugLeds) {
		super(fragConfig, tossimFlag, debugLeds);
		this.op = op;
		this.plan = plan;
		this.id = CodeGenUtils.generateOperatorInstanceName(op, this.site);
    }

    @Override
    public String toString() {
	return this.getID();
    }

    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {

    	try {
			final HashMap<String, String> replacements = new HashMap<String, String>();
			
			replacements.put("__OPERATOR_DESCRIPTION__", this.op.toString()
		    		.replace("\"", ""));
			replacements.put("__OUTPUT_TUPLE_TYPE__", CodeGenUtils
				.generateOutputTupleType(this.op));
			replacements.put("__OUT_QUEUE_CARD__", new Long(
				 this.op.getOutputQueueCardinality(
					this.plan.getRT().getSite(
						this.site.getID()), this.plan.getDAF())).toString());
		    replacements.put("__FULL_ACQUIRE_PREDICATES__", 
		    		getNescText(op.getLogicalOperator().getPredicate()));
		    replacements.put("__ACQUIRE_PREDICATES__", CodeGenUtils.getNescText(
		    		op.getLogicalOperator().getPredicate(), "", null,
		    		((AcquireOperator)op.getLogicalOperator()).getAcquiredAttributes(), null));
		    
		    if (this.debugLeds) {
				replacements.put("__NESC_DEBUG_LEDS__", "call Leds.led0Toggle();");		
			} else {
				replacements.put("__NESC_DEBUG_LEDS__", "");
			}
		    
			this.doTupleConstruction(replacements);
		
			final String outputFileName =
				generateNesCOutputFileName(outputDir, this.getID());
			
			this.doGetDataMethods(replacements);
			writeNesCFile(TinyOSGenerator.NESC_COMPONENTS_DIR + "/acquire.nc",
						outputFileName, replacements);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e);
    	}
    }

    /**
     * Writes the methods to actually acquire the data.
     * 
     * @param replacements Values to be replaced in the tamplates
     */
    private void doGetDataMethods(final Map<String, String> replacements) {
    	final List<Attribute> sensedAttribs = 
    		((AcquireOperator)op.getLogicalOperator()).
    		getInputAttributes();
    	final StringBuffer getDataBuff = new StringBuffer();
    	final StringBuffer declsBuff = new StringBuffer();
    	for (int i = sensedAttribs.size()-1; i >= 0; i--) {
    		declsBuff.append("\tuint16_t reading" + i + ";\n");
    		declsBuff.append("\tbool acquiring" + i + " = FALSE;\n");
    		if (i > 0) {
    			getDataBuff.append("\tvoid task getReading" + i + "Task()\n");
    			getDataBuff.append("\t{\n");
    			getDataBuff.append("\t\tatomic\n");    		    		
    			getDataBuff.append("\t\t\t{\n");
    			getDataBuff.append("\t\t\tacquiring" + i 
    					+ " = TRUE;\n");    		
    			getDataBuff.append("\t\t\t}\n");
    			getDataBuff.append("\t\tcall Read"+ i + ".read();\n");
    			
    			getDataBuff.append("\t}\n\n");
    		}
    		
    	    getDataBuff.append("\tevent void Read" + i + 
	    	    	".readDone(error_t result, uint16_t data)\n");
    	    getDataBuff.append("\t{\n");
    	    getDataBuff.append("\t\tif (acquiring" + i + ") {\n");
    	    getDataBuff.append("\t\t\tatomic\n");
    	    getDataBuff.append("\t\t\t{\n");
    	    getDataBuff.append("\t\t\t\tacquiring" + i + " = FALSE;\n");    	    
    	    getDataBuff.append("\t\t\t\treading" + i + " = data;\n");
     	    final String attribName = 
     	    	CodeGenUtils.getNescAttrName(sensedAttribs.get(i));
    	    String padding = "";

    	    //makes all the sensor readings be nicely aligned
    	    if (attribName.length() < 16) {
    	    	padding = Utils.pad(" ", 16 - attribName.length());
    	    }

    	    getDataBuff.append("\t\t\t}\n");

    	    if (i + 1 < sensedAttribs.size()) {
    	    	getDataBuff.append("\t\t\tpost getReading" + (i + 1) + "Task();\n");
    	    } else {
    	    	getDataBuff.append("\t\t\tpost constructTupleTask();\n");
    	    }
    	    getDataBuff.append("\t\t}\n");
    	    
    	    getDataBuff.append("\t}\n\n");
    	}
    	replacements.put("__GET_DATA_METHODS__", getDataBuff.toString());
    	replacements.put("__READING_VAR_DECLS__", declsBuff.toString());
    }

    /**
     * Writes the methods to actually acquire the data.
     * 
     * @param replacements Values to be replaced in the tamplates
     */
    private void doGetEmptyDataMethods(final Map<String, String> replacements) {
    	final List<Attribute> sensedAttribs = 
    		((AcquireOperator)op.getLogicalOperator()).getInputAttributes();
    	final StringBuffer getDataBuff = new StringBuffer();
    	for (int i = sensedAttribs.size() - 1; i >= 0; i--) {
    	    getDataBuff.append("\tasync event result_t ADC" + i
    		    + ".dataReady(uint16_t data)\n");
    	    getDataBuff.append("\t{\n");
    	    getDataBuff.append("\t}\n\n");
    	}
    	replacements.put("__GET_DATA_METHODS__", getDataBuff.toString());
    }

    /**
     * Generates the text for tuple construction.
     * @param replacements Values to be replaced in the tamplates
     * @throws CodeGenerationException Error if Attribute not acquired.
     */
    private void doTupleConstruction(final Map<String, 
    		String> replacements) throws CodeGenerationException {
    	final StringBuffer tupleConstructionBuff = new StringBuffer();
    	final List <Expression> expressions = 
    		op.getLogicalOperator().getExpressions();
    	final List <Attribute> attributes = op.getLogicalOperator().getAttributes();
    	
    	String comma = "";
    	StringBuffer tupleStrBuff1 = new StringBuffer();
    	StringBuffer tupleStrBuff2 = new StringBuffer();
    	
    	for (int i = 0; i < expressions.size(); i++) {
    		Expression expression = expressions.get(i);
    		if (expression.isConstant()) {
    			throw new CodeGenerationException("Constant values in the " +
    					"select clause are not implemented for in-network " +
    					"evaluation.");
    		}
    		String attrName = CodeGenUtils.getNescAttrName(attributes.get(i));

  			tupleConstructionBuff.append("\t\t\t\toutQueue[outTail]." 
        			+ attrName + "=" + getNescText(expression) + ";\n");
  
	  		if (attributes.get(i) instanceof EvalTimeAttribute ||
	  				attributes.get(i) instanceof IDAttribute
	  					|| attributes.get(i) instanceof TimeAttribute ) {
  	  			tupleStrBuff1.append(comma+attrName+"=%d"); 
  			} else {
  	  			tupleStrBuff1.append(comma+attrName+"=%g");
  			}
  			tupleStrBuff2.append(comma+"outQueue[outTail]."+attrName);
  			comma = ",";
    	}
    	
    	replacements.put("__CONSTRUCT_TUPLE__", tupleConstructionBuff
    			.toString());
    	replacements.put("__CONSTRUCT_TUPLE_STR__", "\"ACQUIRE: ("+tupleStrBuff1.toString()+")\\n\","+tupleStrBuff2.toString());    	
    }
    
    /**
     * Generates nescText based on ReadingX.
     * @param expression Expression to get nesc call for.
     * @return The text to be used in the Nesc code.
     * @throws CodeGenerationException
     * 
     * See also CodeGenUtils.getNescTExt
     */
	private String getNescText(final Expression expression)
			throws CodeGenerationException {
	    if (expression instanceof EvalTimeAttribute) {
	    	return "currentEvalEpoch";
	    }
	    if (expression instanceof TimeAttribute) {
	    	return "currentEvalEpoch";
	    }
	    if (expression instanceof IDAttribute) {
	    	return "TOS_NODE_ID";
	    }
//TOOD: Add LocalTime attribute back in at some point
//	    if (expression instanceof LocalTimeAttribute) {
//	    	return "call LocalTime.get()";
//	    }
		if (expression instanceof DataAttribute) {
			try {
				return "(float) reading" + ((AcquireOperator)op.
				getLogicalOperator()).getInputAttributeNumber(expression);
			} catch (OptimizationException e) {
				throw new CodeGenerationException(e);
			} 
		}	
		if (expression instanceof MultiExpression) {
			MultiExpression multi = (MultiExpression) expression;
			Expression[] expressions = multi.getExpressions(); 
			String output = "(" + getNescText(expressions[0]);
			for (int i = 1; i < expressions.length; i++) {
				output = output + multi.getMultiType().getNesC() 
					+ getNescText(expressions[i]);
			}
			return output + ")";
		}
		if (expression instanceof NoPredicate) {
			return "TRUE";
		}
		if (expression instanceof IntLiteral) {
			return expression.toString();
		}		
		throw new CodeGenerationException("Missing code. Expression "
			+ expression);	
	}
}
