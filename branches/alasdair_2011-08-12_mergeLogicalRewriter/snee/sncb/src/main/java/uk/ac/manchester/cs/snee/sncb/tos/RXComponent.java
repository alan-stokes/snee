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

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;

public class RXComponent extends NesCComponent {

	/**
	 * The fragment that tuples originate from (i.e., the tuple type)
	 */
    Fragment sourceFrag;

	/**
	 * The fragment that tuples are being sent to
	 */
    Site destSite;

    /**
     * The site when the instance of the destination fragment is located.  
     * This is required as there may be more than one instance of the destination fragment.
     */
    Fragment destFrag;

    /**
     * The site that this component's tuples are being received from, i.e., the previous hop
     * towards the final destination site. This is the same as the child site in the routing
     * tree.
     */
    Site txSite;

    /**
     * The query plan for which code is being generated.
     */
    SensorNetworkQueryPlan plan;


    private final String templateFileName = TinyOSGenerator.NESC_COMPONENTS_DIR
	    + "/rx.nc";

    public RXComponent(final Fragment sourceFrag,
    	    final Fragment destFrag, final Site destSite,
    	    final Site txSite, final NesCConfiguration config, 
    	    final SensorNetworkQueryPlan plan,
    	    boolean tossimFlag, boolean debugLeds,
    	    CodeGenTarget target) {
		super(config, tossimFlag, debugLeds, target);
		this.instanceOfGeneric = true;
		this.sourceFrag = sourceFrag;
		this.destSite = destSite;
		this.destFrag = destFrag;
		this.txSite = txSite;
		this.id = this.generateName();
		this.plan = plan;
    }

    @Override
    public String toString() {
	return this.getID();
    }

    private String generateName() {
	return "rx_" + "frag" + this.sourceFrag.getID() + "frag"
		+ this.destFrag.getID() + "n" + this.destSite.getID() + "_tx"
		+ this.txSite.getID() + "rx" + this.site.getID() 
		+ "_Site" + this.site.getID()+ "P";
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {

    	try {
		final HashMap<String, String> replacements = new HashMap<String, String>();
	
		replacements.put("__MESSAGE_PTR_TYPE__", CodeGenUtils
			.generateMessagePtrType(this.sourceFrag));
		replacements.put("__MESSAGE_TYPE__", CodeGenUtils
				.generateMessageType(this.sourceFrag));
		
		if (this.debugLeds) {
			replacements.put("__NESC_DEBUG_LEDS__", "call Leds.led1Toggle();");		
		} else {
			replacements.put("__NESC_DEBUG_LEDS__", "");
		}
		
		final String outputFileName = generateNesCOutputFileName(outputDir, this.getID());
	
		super
			.writeNesCFile(this.templateFileName, outputFileName,
				replacements);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e);
    	}
    }

	public Site getTxSite() {
		return this.txSite;
	}

}
