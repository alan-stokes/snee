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

import java.util.HashMap;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

public class JoinComponent extends NesCComponent {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4291321427846704916L;

	SensornetNestedLoopJoinOperator op;

    SensorNetworkQueryPlan plan;

    public JoinComponent(final SensornetNestedLoopJoinOperator op, final SensorNetworkQueryPlan plan,
	    final NesCConfiguration fragConfig,
	    boolean tossimFlag, boolean debugLeds, CodeGenTarget target) {
	super(fragConfig, tossimFlag, debugLeds, target);
	this.op = op;
	this.plan = plan;
	this.id = CodeGenUtils.generateOperatorInstanceName(op, this.site);
    }

    @Override
    public String toString() {
	return this.getID();
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {

    	try {
			final HashMap<String, String> replacements = new HashMap<String, String>();
			replacements.put("__OPERATOR_DESCRIPTION__", this.op.toString()
				.replace("\"", ""));
			replacements.put("__OUTPUT_TUPLE_TYPE__", CodeGenUtils
				.generateOutputTupleType(this.op));
			replacements.put("__OUT_QUEUE_CARD__", new Long(
				op.getOutputQueueCardinality(
					(Site) this.plan.getRT().getSite(
						this.site.getID()), this.plan.getDAF())).toString());
			replacements.put("__CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
				.generateOutputTuplePtrType(this.op.getLeftChild()));
		
			replacements.put("__LEFT_CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
				.generateOutputTuplePtrType(this.op.getLeftChild()));
			replacements.put("__RIGHT_CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
				.generateOutputTuplePtrType(this.op.getRightChild()));
		    replacements.put("__JOIN_PREDICATES__", CodeGenUtils.getNescText(
		    		((JoinOperator)op.getLogicalOperator()).getPredicate(),
		    		"leftInQueue[leftInHead].",
		    		"rightInQueue[tmpRightInHead].", 
		    		op.getLeftChild().getAttributes(), 
		    		op.getRightChild().getAttributes(), target));
			
			final StringBuffer tupleConstructionBuff = generateTupleConstruction();
			replacements.put("__CONSTRUCT_TUPLE__", tupleConstructionBuff.toString());
		
			final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
			writeNesCFile(TinyOSGenerator.NESC_COMPONENTS_DIR + "/join.nc", outputFileName,
				replacements);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e.getLocalizedMessage(), e);
    	}
    }

    /**
     * Generates the tuple construction.
     * 
     * @return NesC tuple construction code.
     * @throws CodeGenerationException 
     */
    private StringBuffer generateTupleConstruction() throws CodeGenerationException {
		final StringBuffer tupleConstructionBuff = new StringBuffer();
		final List <Attribute> attributes = op.getAttributes();
		final List <Attribute> leftInput = op.getLeftChild().getAttributes();
		final List <Attribute> rightInput = op.getRightChild().getAttributes();
		final List <Expression> expressions = op.getExpressions();
    	
		for (int i = 0; i < attributes.size(); i++) {
			String attrName 
				= CodeGenUtils.getNescAttrName(attributes.get(i));
			String expressionText;
				expressionText = CodeGenUtils.getNescText(
					expressions.get(i), "leftInQueue[leftInHead].", 
					"rightInQueue[tmpRightInHead].", 
					leftInput, rightInput, target);
				
				tupleConstructionBuff.append("\t\t\t\t\toutQueue[outTail]."
    				+ attrName + "=" + expressionText + ";\n");
		}		
		return tupleConstructionBuff;
    } 

}
