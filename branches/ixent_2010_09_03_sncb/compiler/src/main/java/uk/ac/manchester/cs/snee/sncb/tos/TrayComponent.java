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


import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;

public class TrayComponent extends NesCComponent {

    Fragment sourceFrag;

    Fragment destFrag;

    String destSiteID;
    
    Site currentSite;

    QueryPlan plan;

    QoSSpec qos;
    
    //Site site;

    public TrayComponent(final Fragment sourceFrag, final Fragment destFrag,
	    final String destSiteID, final Site currentSite, 
	    final NesCConfiguration config, final QueryPlan plan,
	    final QoSSpec qos, int tosVersion, boolean tossimFlag) {
	super(config, tosVersion, tossimFlag);
	this.sourceFrag = sourceFrag;
	this.destFrag = destFrag;
	this.destSiteID = destSiteID;
	this.currentSite = currentSite;
	this.id = generateName(sourceFrag, destFrag, this.site.getID(), destSiteID, tosVersion);
	this.plan = plan;
	this.qos = qos;
    }

    /**
     * A tray name takes the following form:
     * 
     * 		trayF{sourceFragID}_F{destFragID}n{destSiteID}_n{currentSiteID}P
     * 
     * Trays are therefore uniquely identified according to the input tuple default type name, 
     * the destination fragment id, the destination site id, and the current site id.
     * Trays with tuples of the same type which have the same destination fragment id and destination 
     * site id are therefore merged into a single tray (regardless of their source site).  Note that 
     * theoretically it is possible for tuples of the type with the same destination fragment to have 
     * different destination sites (e.g., in the case of partitioned parallel join), which is why
     * we don't merge tuples with the same destination fragment but different destination sites.
     */
    public static String generateName(final Fragment sourceFrag,
	    final Fragment destFrag, final String currentSiteID,
	    final String destSiteID, int tosVersion) {
	if (sourceFrag.isRecursive()) {
	    if (tosVersion == 1) {
		return "trayF" + sourceFrag.getChildFragments().get(0).getID()
			+ "_F" + destFrag.getID() + "n" + destSiteID + "_n"
			+ currentSiteID + "M";
	    } else {
		return "trayF" + sourceFrag.getChildFragments().get(0).getID()
			+ "_F" + destFrag.getID() + "n" + destSiteID + "_n"
			+ currentSiteID + "P";
	    }
	} else {
	    if (tosVersion == 1) {
		return "trayF" + sourceFrag.getID() + "_F" + destFrag.getID()
			+ "n" + destSiteID + "_n" + currentSiteID + "M";
	    } else {
		return "trayF" + sourceFrag.getID() + "_F" + destFrag.getID()
			+ "n" + destSiteID + "_n" + currentSiteID + "P";
	    }
	}
    }

    @Override
    public String toString() {
	return this.getID();
    }
    
    
    /**
     * Retrieves the required subtray size.
     * @param site Site For which the Size is to be calculated.
     * @return Size of subtray needed.
     */
    private int getSubTraySize(final Site site) {
    	DAF daf = this.plan.getDAF();
    	final Operator source = 
    		daf.getFragment(this.sourceFrag.getID()).getRootOperator();
       	int subTraySize = 0;
       	if (site.hasFragmentAllocated(sourceFrag)) {
       		subTraySize = source.getCardinality(CardinalityType.MAX, 
    				site, daf);
    	}
     	final int sites = site.getInDegree();
    	for (int i = 0; i < sites; i++) {
     		subTraySize += getSubTraySize(site.getChild(i));
    	}
    	return subTraySize;
    }
    
    public void writeNesCFile(final String outputDir)
	    throws IOException, CodeGenerationException {

	final long bufferingFactor = this.plan.getBufferingFactor();
	final int subTraySize = getSubTraySize(currentSite);

	final Operator sourceFragRootOp = this.plan.getDAF().getFragment(
		this.sourceFrag.getID()).getRootOperator();
	final int tupleSize = CodeGenUtils.outputTypeSize.get(CodeGenUtils
		.generateOutputTupleType(sourceFragRootOp));

	final HashMap<String, String> replacements = new HashMap<String, String>();

	if (!this.sourceFrag.isRecursive()) {
	    replacements.put("__TUPLE_TYPE__", "TupleFrag"
		    + this.sourceFrag.getID());
	    replacements.put("__TUPLE_PTR_TYPE__", "TupleFrag"
		    + this.sourceFrag.getID() + "Ptr");
	} else {
	    replacements.put("__TUPLE_TYPE__", "TupleFrag"
		    + this.sourceFrag.getChildFragments().get(0).getID());
	    replacements.put("__TUPLE_PTR_TYPE__", "TupleFrag"
		    + this.sourceFrag.getChildFragments().get(0).getID()
		    + "Ptr");
	}
	replacements.put("__NUM_SUBTRAYS__", new Long(bufferingFactor)
		.toString());
	replacements.put("__SUBTRAY_SIZE__", new Integer(subTraySize)
		.toString());
	replacements.put("__MAXTUPLESINMSG__", new Integer(ExchangePart
		.computeTuplesPerMessage(tupleSize)).toString());
	replacements.put("__MODULE_NAME__ ", this.getID());
	//	if (Settings.NESC_MAX_DEBUG_STATEMENTS_IN_TRAY)
	replacements.put("__MAX_DEBUG__ ", "");
	replacements.put("__MAX_DEBUG1__ ", "//");
	//	else
	//	replacements.put("__MAX_DEBUG__ ", "//");
	final StringBuffer tupleConstructionBuff = new StringBuffer();
	final StringBuffer tupleConstructionBuff2 = new StringBuffer();
	final ArrayList <Attribute> attributes = sourceFragRootOp.getAttributes();
	for (int i = 0; i < attributes.size(); i++) {
	    String attrName = CodeGenUtils.getNescAttrName(attributes.get(i));
	    if (Settings.MEASUREMENTS_IGNORE_IN.contains("tray") && attrName.contains("ignore")) {
		    tupleConstructionBuff.append("\t\t\t\t//SKIPPING tray[subTrayNum][trayTail]."
				    + attrName + "=inQueue[inHead]." + attrName + ";\n");
			    tupleConstructionBuff2
				    .append("\t\t\t\t\t//SKIPPING tray[subTrayNum][trayTail]." + attrName
					    + "=message[inHead]." + attrName + ";\n");
	    } else {
	    	tupleConstructionBuff.append("\t\t\t\ttray[subTrayNum][trayTail]."
			    + attrName + "=inQueue[inHead]." + attrName + ";\n");
		    tupleConstructionBuff2
			    .append("\t\t\t\t\ttray[subTrayNum][trayTail]." + attrName
				    + "=message[inHead]." + attrName + ";\n");
	    }
	}
	replacements.put("__TUPLE_CONSTRUCTION__", tupleConstructionBuff
		.toString());
	replacements.put("__TUPLE_CONSTRUCTION2__", tupleConstructionBuff2
		.toString());

	final String outputFileName 
		= generateNesCOutputFileName(outputDir, this.getID());

	if (Settings.MEASUREMENTS_REMOVE_OPERATORS.contains("everything")) {
		writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/measurements/empty_tray.nc", 
				outputFileName,	replacements);				
	} else if (Settings.MEASUREMENTS_REMOVE_OPERATORS.contains("trayall")) {
		if ((Settings.MEASUREMENTS_THIN_OPERATORS.contains("producer1"))) {
			writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/measurements/empty_tray2.nc", 
				outputFileName,	replacements);
		} else {
			writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/measurements/empty_tray.nc", 
					outputFileName,	replacements);			
		}
	} else if (Settings.MEASUREMENTS_REMOVE_OPERATORS.contains("tray" + this.sourceFrag.getID())) {
		writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/measurements/empty_tray.nc", 
			outputFileName,	replacements);		
	} else {
		writeNesCFile(NesCGeneration.NESC_MODULES_DIR + "/tray.nc", outputFileName,
			replacements);
	}
    }

/*	if (exchComp.getComponentType()==exchComp.EXCHANGE_PRODUCER) {
		System.out.println("Use cuurent Site = "+currentSite.getID());			
	}
	if (exchComp.getComponentType()== exchComp.EXCHANGE_RELAY) {
		System.out.println("Relay on Site = "+currentSite.getID());			
	}
	if (exchComp.getComponentType()== exchComp.EXCHANGE_RELAY) {
		System.out.println("Site = "+currentSite.getID());
		System.out.println("inputs = "+currentSite.getInDegree());
	}
*/
}
