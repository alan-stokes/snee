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
import java.util.ArrayList;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;

public class JoinComponent extends NesCComponent implements TinyOS1Component,
	TinyOS2Component {

    JoinOperator op;

    QueryPlan plan;

    QoSSpec qos;

    Fragment frag;

    public JoinComponent(final JoinOperator op, final QueryPlan plan,
	    final QoSSpec qos, final NesCConfiguration fragConfig,
	    int tosVersion, boolean tossimFlag) {
	super(fragConfig, tosVersion, tossimFlag);
	this.op = op;
	this.frag = op.getContainingFragment();
	this.plan = plan;
	this.qos = qos;
	this.id = CodeGenUtils.generateOperatorInstanceName(op, this.frag,
		this.site, tosVersion);
    }

    @Override
    public String toString() {
	return this.getID();
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws IOException, CodeGenerationException {

	final HashMap<String, String> replacements = new HashMap<String, String>();
	replacements.put("__OPERATOR_DESCRIPTION__", this.op.getText(false)
		.replace("\"", ""));
	replacements.put("__OUTPUT_TUPLE_TYPE__", CodeGenUtils
		.generateOutputTupleType(this.op));
	replacements.put("__OUT_QUEUE_CARD__", new Long(
		op.getOutputQueueCardinality(
			(Site) this.plan.getRoutingTree().getNode(
				this.site.getID()), this.plan.getDAF())).toString());
	replacements.put("__CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
		.generateOutputTuplePtrType(this.op.getInput(0)));

	replacements.put("__LEFT_CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
		.generateOutputTuplePtrType(this.op.getInput(0)));
	replacements.put("__RIGHT_CHILD_TUPLE_PTR_TYPE__", CodeGenUtils
		.generateOutputTuplePtrType(this.op.getInput(1)));
    replacements.put("__JOIN_PREDICATES__", CodeGenUtils.getNescText(
    		op.getPredicate(),"leftInQueue[leftInHead].",
    		"rightInQueue[tmpRightInHead].", op.getInput(0).getAttributes(), 
    		op.getInput(1).getAttributes()));
	
	final StringBuffer tupleConstructionBuff = generateTupleConstruction();
	replacements.put("__CONSTRUCT_TUPLE__", tupleConstructionBuff.toString());

	final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
	writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/join.nc", outputFileName,
		replacements);
    }

    /**
     * Generates the tuple construction.
     * 
     * @return NesC tuple construction code.
     */
    private StringBuffer generateTupleConstruction() {
    	final StringBuffer tupleConstructionBuff = new StringBuffer();
    	final ArrayList <Attribute> attributes = op.getAttributes();
    	final ArrayList <Attribute> leftInput = op.getInput(0).getAttributes();
    	final ArrayList <Attribute> rightInput = op.getInput(1).getAttributes();
    	final ArrayList <Expression> expressions = op.getExpressions();
		try {
			for (int i = 0; i < attributes.size(); i++) {
				String attrName 
					= CodeGenUtils.getNescAttrName(attributes.get(i));
				String expressionText;
					expressionText = CodeGenUtils.getNescText(
						expressions.get(i), "leftInQueue[leftInHead].", 
						"rightInQueue[tmpRightInHead].", 
						leftInput, rightInput);
					
					//IG: Yuck - Christian I need you to sort this out!
					//I did this to make the nesc code compile with epochs
					expressionText = expressionText.replace(".evalTime", ".evalEpoch");
					expressionText = expressionText.replace("_time", "_epoch");
					
					tupleConstructionBuff.append("\t\t\t\t\toutQueue[outTail]."
	    				+ attrName + "=" + expressionText + ";\n");
			}		
			return tupleConstructionBuff;
		} catch (CodeGenerationException e) {
			Utils.handleCriticalException(e);
			return null;
		}
    } 

}
