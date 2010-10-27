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
import java.util.ArrayList;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

public class TXT1Component extends NesCComponent implements TinyOS1Component {

    Site currentSite;

    Site parentSite;

    SensorNetworkQueryPlan plan;

    ArrayList<ExchangePart> exchComponents = new ArrayList<ExchangePart>();

    private final String templateFileName = TinyOSGenerator.NESC_COMPONENTS_DIR
	    + "/tx.nc";

	private CostParameters costParams;

    public TXT1Component(final Site sourceSite, final Site destSite,
	    final NesCConfiguration config, final SensorNetworkQueryPlan plan,
	    boolean tossimFlag, CostParameters costParams, boolean debugLeds) {
	super(config, 1, tossimFlag, debugLeds);
	this.currentSite = sourceSite;
	this.parentSite = destSite;
	this.id = this.generateName();
	this.plan = plan;
	this.costParams = costParams;
    }

    @Override
    public String toString() {
	return this.getID();
    }

    private String generateName() {
	return "tx_n" + this.currentSite.getID() + "_n"
		+ this.parentSite.getID() + "M";
    }

    public void addExchangeComponent(final ExchangePart exchComp) {
	boolean found = false;

	for (int i = 0; i < this.exchComponents.size(); i++) {
	    if ((this.exchComponents.get(i).getSourceFrag() == exchComp
		    .getSourceFrag())
		    && (this.exchComponents.get(i).getDestFrag() == exchComp
			    .getDestFrag())
		    && (this.exchComponents.get(i).getDestSite() == exchComp
			    .getDestSite())) {
		found = true;
		break;
	    }

	}

	if (!found) { //we don't want more than on exchange component with same dest site/frag
	    this.exchComponents.add(exchComp);
	}
    }

    private String generatePrefix(final Fragment sourceFrag,
	    final Fragment destFrag, final Site destSite) {
	return "F" + sourceFrag.getID() + "_F" + destFrag.getID() + "_n"
		+ destSite.getID();
    }

    private String generateInQueueName(final Fragment sourceFrag,
	    final Fragment destFrag, final Site destSite) {
	return this.generatePrefix(sourceFrag, destFrag, destSite) + "InQueue";
    }

    private String generatePacketName(final Fragment sourceFrag,
	    final Fragment destFrag, final Site destSite) {
	return this.generatePrefix(sourceFrag, destFrag, destSite) + "Packet";
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {

    	try {
		final HashMap<String, String> replacements = new HashMap<String, String>();
	
		replacements.put("__MODULE_NAME__", this.getID());
		replacements.put("__HEADER__", this.configuration
			.generateModuleHeader(this.getID()));
	
		final String siteID = this.currentSite.getID();
		final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
	
		final StringBuffer txMethodsBuff = new StringBuffer();
		final StringBuffer declsBuffer = new StringBuffer();
	
		boolean first = true;
		for (int i = 0; i < this.exchComponents.size(); i++) {
		    final ExchangePart exchComp = this.exchComponents.get(i);
		    final Fragment sourceFrag = exchComp.getSourceFrag();
		    final Fragment destFrag = exchComp.getDestFrag();
		    final Site destSite = exchComp.getDestSite(); //nb: final destination, not next hop
	
		    final String inQueueType = CodeGenUtils
			    .generateOutputTuplePtrType(sourceFrag);
		    final String inQueueName = this.generateInQueueName(sourceFrag,
			    destFrag, destSite);
		    final String packetType = CodeGenUtils
			    .generateMessageType(sourceFrag);
		    final String packetTypePtr = CodeGenUtils
			    .generateMessagePtrType(sourceFrag);
		    final String packetName = this.generatePacketName(sourceFrag,
			    destFrag, destSite);
		    final String trayCall = CodeGenUtils
			    .generateGetTuplesInterfaceInstanceName(sourceFrag,
				    destFrag, destSite.getID(), this.currentSite
					    .getID());
	
		    declsBuffer.append("\t" + inQueueType + " " + inQueueName + ";\n");
		    declsBuffer.append("\t" + packetTypePtr + " " + packetName + ";\n");
	
		    if (first) {
			replacements.put("__FIRST_TRAY_CALL__", trayCall);
			replacements.put("__FIRST_MESSAGE_PTR_TYPE_INSTANCE__",
				packetName);
			replacements.put("__FIRST_MESSAGE_PTR_TYPE__", packetTypePtr);
			replacements.put("__BUFFERING_FACTOR__", new Long(this.plan
				.getBufferingFactor()).toString());
			replacements.put("__EVALUATION_INTERVAL__", new Long(this.plan
				.getAcquisitionInterval_ms()).toString());
			replacements.put("__FROM_SITE__", this.currentSite.getID());
			replacements.put("__TO_SITE__", this.parentSite.getID());
			first = false;
		    }
	
		    final HashMap<String, String> txMethodsReplacements = new HashMap<String, String>();
		    txMethodsReplacements.put("__TRAY_PREFIX__", this.generatePrefix(
			    sourceFrag, destFrag, destSite));
		    txMethodsReplacements.put("__TRAY_CALL__", trayCall);
		    txMethodsReplacements.put("__SEND_INTERFACE__", CodeGenUtils
			    .generateUserSendInterfaceName(sourceFrag, destFrag,
				    destSite.getID()));
		    txMethodsReplacements.put("__PARENT_ID__", this.parentSite.getID());
		    txMethodsReplacements.put("__TUPLE_MESSAGE_TYPE__", packetType);
	
		    final int tupleSize = new Integer(CodeGenUtils.outputTypeSize
			    .get(CodeGenUtils.generateOutputTupleType(sourceFrag)));
		    final int numTuplesPerMessage = ExchangePart
			    .computeTuplesPerMessage(tupleSize, costParams);
		    assert (numTuplesPerMessage > 0);
		    txMethodsReplacements.put("__TUPLES_PER_PACKET__", new Integer(
			    numTuplesPerMessage).toString());
	
		    if (i + 1 < this.exchComponents.size()) {
			final Fragment nextSourceFrag = this.exchComponents.get(i + 1)
				.getSourceFrag();
			final Fragment nextDestFrag = this.exchComponents.get(i + 1)
				.getDestFrag();
			final Site nextDestSite = this.exchComponents.get(i + 1)
				.getDestSite();
	
			final String nextPacketType = CodeGenUtils
				.generateMessageType(nextSourceFrag);
			final String nextPacketName = this.generatePacketName(
				nextSourceFrag, nextDestFrag, nextDestSite);
			final String nextTrayCall = CodeGenUtils
				.generateGetTuplesInterfaceInstanceName(nextSourceFrag,
					nextDestFrag, nextDestSite.getID(),
					this.currentSite.getID());
	
			final StringBuffer nextTrayCallBuff = new StringBuffer();
			nextTrayCallBuff
				.append("\t\t\t\t//Nothing more to send from this tray\n");
			nextTrayCallBuff
				.append("\t\t\t\t//Request data from the next tray\n");
			nextTrayCallBuff
				.append("\t\t\t\tdbg(DBG_USR3,\"Calling next tray: "
					+ nextTrayCall + "\\n\");\n");
			nextTrayCallBuff.append("\t\t\t\tbFactorCount = 0;\n");
			nextTrayCallBuff.append("\t\t\t\ttuplePacketPos = 0;\n");
	
			nextTrayCallBuff.append("\t\t\t\t" + nextPacketName + "= ("
				+ nextPacketType + "*)data.data;\n");
			nextTrayCallBuff.append("\t\t\t\tevalEpoch=currentEvalEpoch;\n");
			nextTrayCallBuff.append("\t\t\t\tcall " + nextTrayCall
				+ ".requestData(evalEpoch);\n");
			txMethodsReplacements.put("__NEXT_TRAY_CALL__",
				nextTrayCallBuff.toString());
		    } else {
			txMethodsReplacements
				.put("__NEXT_TRAY_CALL__",
					"\t\t\t\t//No more trays to request data from; stop here\n");
		    }
		    txMethodsReplacements.put("__TRAY_PREFIX_TUPLE_PTR_TYPE__",
			    inQueueType);
	
		    txMethodsBuff.append(generateNesCMethods(TinyOSGenerator.NESC_COMPONENTS_DIR
			    + "/tx_methods.nc", txMethodsReplacements));
		}
	
		replacements.put("__DECLS__", declsBuffer.toString());
		replacements.put("__EVALUATION_INTERVAL__", new Integer((int) this.plan
			.getAcquisitionInterval_ms()).toString());
		replacements.put("__TX_METHODS__", txMethodsBuff.toString());
	
		super
			.writeNesCFile(this.templateFileName, outputFileName,
				replacements);
	    } catch (Exception e) {
	    	throw new CodeGenerationException(e);
	    }
    }
}
