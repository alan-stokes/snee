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

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

public class DeliverComponent extends NesCComponent implements
	TinyOS1Component, TinyOS2Component {

	SensornetDeliverOperator op;

    SensorNetworkQueryPlan plan;
    
    CostParameters costParams;

    public DeliverComponent(final SensornetDeliverOperator op, final SensorNetworkQueryPlan plan, 
    final NesCConfiguration fragConfig, int tosVersion, boolean tossimFlag, boolean debugLeds, CostParameters costParams) {

		super(fragConfig, tosVersion, tossimFlag, debugLeds);
		this.op = op;
		this.plan = plan;
		this.id = CodeGenUtils.generateOperatorInstanceName(op, this.site, tosVersion);
		this.costParams = costParams;
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
		
			if (tosVersion==1) {
				replacements.put("__ITOA_DECL__", "#include \"itoa.h\"");
			}
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
			replacements.put("__MESSAGE_TYPE__", CodeGenUtils
					.generateMessageType(this.op.getLeftChild()));
			replacements.put("__MESSAGE_PTR_TYPE__", CodeGenUtils
					.generateMessagePtrType(this.op.getLeftChild()));
		
			//int tuplesPerPacket= Settings.NESC_MAX_MESSAGE_PAYLOAD_SIZE/((new Integer(CodeGenUtils.outputTypeSize.get(CodeGenUtils.generateOutputTupleType(sourceFrag)).toString()))+ Settings.NESC_PAYLOAD_OVERHEAD);
			final int tupleSize = new Integer(CodeGenUtils.outputTypeSize
				.get(CodeGenUtils.generateOutputTupleType(this.op.getContainingFragment())));
			//	int tuplesPerPacket =(int)Math.floor((Settings.NESC_MAX_MESSAGE_PAYLOAD_SIZE - (Settings.NESC_PAYLOAD_OVERHEAD+2)) / (tupleSize+2));
			final int numTuplesPerMessage = ExchangePart
				.computeTuplesPerMessage(tupleSize, this.costParams);
			assert (numTuplesPerMessage > 0);
		
			replacements.put("__PARENT_ID__", "AM_BROADCAST_ADDR");
			replacements.put("__TUPLES_PER_PACKET__", new Integer(
					numTuplesPerMessage).toString());
			replacements.put("__BUFFERING_FACTOR__", ""+this.plan
				.getBufferingFactor());
		
			final StringBuffer displayTupleBuff3 = new StringBuffer();  //serial port
			final StringBuffer displayTupleBuff4 = new StringBuffer();  //serial port
			final StringBuffer displayTupleBuff5 = new StringBuffer();  //serial port str - tinyos 1 only
		
			final List<Attribute> attributes = this.op.getAttributes();
		    String comma = "";
			for (int i = 0; i < attributes.size(); i++) {
				String attrName = CodeGenUtils.getNescAttrName(attributes.get(i));
				String deliverName = CodeGenUtils.getDeliverName(attributes.get(i));
		
			    displayTupleBuff3.append(comma+deliverName+"=%d");
			    displayTupleBuff4.append(comma+"inQueue[inHead]."+attrName);
		
			    displayTupleBuff5.append("\t\t\t\tstrcat(deliverStr, \""
					    		+ comma + deliverName + "=\");\n");
				displayTupleBuff5.append("\t\t\t\titoa(inQueue[inHead]."
					    		+ attrName + ", tmpStr, 10);\n");
			    displayTupleBuff5.append("\t\t\t\tstrcat(deliverStr,tmpStr);\n");
		
		    	comma = ",";
			}
		
			replacements.put("__CONSTRUCT_DELIVER_TUPLE__",
					"\"DELIVER: ("+displayTupleBuff3.toString()+")\\n\","+displayTupleBuff4.toString());
			replacements.put("__CONSTRUCT_DELIVER_TUPLE_STR__",
				displayTupleBuff5.toString());
		
			final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
			writeNesCFile(TinyOSGenerator.NESC_COMPONENTS_DIR + "/deliver.nc",
		    			outputFileName, replacements);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e);
    	}
    }

}
