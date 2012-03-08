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

import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

public class TXComponent extends NesCComponent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1250643464074558997L;

	/**
	 * The fragment that tuples originate from (i.e., the tuple type)
	 */
    Fragment sourceFrag;
	
	/**
	 * The fragment that tuples are being sent to
	 */
    Fragment destFrag;

    /**
     * The site when the instance of the destination fragment is located.  
     * This is required as there may be more than one instance of the destination fragment.
     */
    Site destSite;

    /**
     * The site that this component's tuples are being transmitted to, i.e., the next hop
     * towards the final destination site. This is the same as the parent site in the routing
     * tree.
     */
    Site rxSite;
    
    /**
     * The query plan for which code is being generated.
     */
    SensorNetworkQueryPlan plan;
    
    CostParameters costParams;

    private final String templateFileName = TinyOSGenerator.NESC_COMPONENTS_DIR
	    + "/tx.nc";

    
    public TXComponent(final Fragment sourceFrag,
	    final Fragment destFrag, final Site destSite,
	    final Site rxSite, final NesCConfiguration config, 
	    final SensorNetworkQueryPlan plan,
	    boolean tossimFlag, CostParameters costParams, boolean ledsDebug,
	    CodeGenTarget target) {
	
    	super(config, tossimFlag, ledsDebug, target);
		this.instanceOfGeneric = true;
		assert this.destFrag != null;
		this.sourceFrag = sourceFrag;
    if(this.sourceFrag == null)
      System.out.println("");
		this.destSite = destSite;
		this.destFrag = destFrag;
		this.rxSite = rxSite;
		this.id = this.generateName();
		this.plan = plan;
		this.costParams = costParams;
    }

    @Override
    public String toString() {
	return this.getID();
    }

    private String generateName() {
	return "tx_" + "frag" + this.sourceFrag.getID() + "frag"
		+ this.destFrag.getID() + "n" + this.destSite.getID() + "_tx"
		+ this.site.getID() + "rx" + this.rxSite.getID() 
		+ "_Site" + this.site.getID()+ "P";
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {
    	
    	try {
		final String currentSiteID = this.site.getID();
		final String parentSiteID = this.rxSite.getID();
	
		final HashMap<String, String> replacements = new HashMap<String, String>();
	
		replacements.put("__CURRENT_SITE_ID__", currentSiteID);
		replacements.put("__PARENT_ID__", parentSiteID);
		replacements.put("__BUFFERING_FACTOR__", new Long(this.plan
				.getBufferingFactor()).toString());
		replacements.put("__CHILD_TUPLE_PTR_TYPE__", 
				CodeGenUtils.generateOutputTuplePtrType(this.sourceFrag));
		replacements.put("__MESSAGE_TYPE__", CodeGenUtils
			.generateMessageType(this.sourceFrag));
		replacements.put("__MESSAGE_PTR_TYPE__", CodeGenUtils
				.generateMessagePtrType(this.sourceFrag));
		
		if (this.debugLeds) {
			replacements.put("__NESC_DEBUG_LEDS__", "call Leds.led2Toggle();");		
		} else {
			replacements.put("__NESC_DEBUG_LEDS__", "");
		}
		
		//int tuplesPerPacket= Settings.NESC_MAX_MESSAGE_PAYLOAD_SIZE/((new Integer(CodeGenUtils.outputTypeSize.get(CodeGenUtils.generateOutputTupleType(sourceFrag)).toString()))+ Settings.NESC_PAYLOAD_OVERHEAD);
		int tupleSize = 0;
		String temp = CodeGenUtils.generateOutputTupleType(this.sourceFrag);
		Integer inttemp = CodeGenUtils.outputTypeSize.get(temp);
		if(inttemp  ==  null)
		{
      System.out.println("broken");
      System.out.println("children size = " + this.sourceFrag.getChildFragments().size());
      System.out.println("");
		}
		tupleSize = inttemp;
		
		if(tupleSize == 0)
		{
		  System.out.println("broken");
		  System.out.println("children size = " + this.sourceFrag.getChildFragments().size());
		  System.out.println("");
		}
		//	int tuplesPerPacket =(int)Math.floor((Settings.NESC_MAX_MESSAGE_PAYLOAD_SIZE - (Settings.NESC_PAYLOAD_OVERHEAD+2)) / (tupleSize+2));
		final int numTuplesPerMessage = ExchangePart
			.computeTuplesPerMessage(tupleSize, costParams);
		assert (numTuplesPerMessage > 0);
	
		replacements.put("__TUPLES_PER_PACKET__", new Integer(
			numTuplesPerMessage).toString());
	
		replacements.put("__TUPLE_PTR_TYPE__", CodeGenUtils
			.generateOutputTuplePtrType(this.sourceFrag));
		replacements.put("__HEADER__", this.configuration
			.generateModuleHeader(this.getID()));
		replacements.put("__MODULE_NAME__", this.getID());
	
		final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
	
		super
			.writeNesCFile(this.templateFileName, outputFileName,
				replacements);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e);
    	}
    }
}
