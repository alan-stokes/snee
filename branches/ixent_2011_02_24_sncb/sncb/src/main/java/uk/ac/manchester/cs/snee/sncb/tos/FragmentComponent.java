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

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;

public class FragmentComponent extends NesCComponent implements
	TinyOS2Component {

    Fragment frag;

    NesCConfiguration innerConfig;

    public FragmentComponent(final Fragment frag,
	    final NesCConfiguration outerConfig,
	    boolean tossimFlag) {
    	
		super(outerConfig, tossimFlag);
		this.frag = frag;
		this.id = generateName(frag, this.site);
    }

    @Override
    public String toString() {
	return this.getID();
    }

    public static String generateName(final Fragment frag,
	    final Site currentSite) {

    	return "Frag" + frag.getID() + "n" + currentSite.getID() + "P";
    }

    public void setInnerConfig(final NesCConfiguration fragConfig) {
	this.innerConfig = fragConfig;
    }

    public NesCConfiguration getInnerConfig() {
	return this.innerConfig;
    }

    @Override
    public void writeNesCFile(final String outputDir)
	    throws CodeGenerationException {

    	try {
			//TODO: Take the configuration of inner component and create a wiring code
			this.innerConfig.instantiateComponents(outputDir);
    	} catch (Exception e) {
    		throw new CodeGenerationException(e.getLocalizedMessage(),e);
    	}
    }

}
