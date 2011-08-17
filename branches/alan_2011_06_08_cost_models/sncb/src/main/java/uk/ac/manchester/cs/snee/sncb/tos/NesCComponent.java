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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public abstract class NesCComponent extends NodeImplementation implements Node {

    String description;
    
    boolean tossimFlag;
    
    CodeGenTarget target;

    boolean systemComponent = false;

	boolean debugLeds;
    
    protected NesCComponent() {
	super();
	this.configuration = null;
    }

    protected NesCComponent(final NesCConfiguration config, boolean tossimFlag, boolean debugLeds, CodeGenTarget target) {
		super();
		this.configuration = config;
		this.site = config.getSite();
		this.tossimFlag = tossimFlag;
		this.target = target;
		this.debugLeds = debugLeds;
    }

    public NesCComponent(final NesCConfiguration config, boolean tossimFlag) {
    	super();
		this.configuration = config;
		this.site = config.getSite();
		this.tossimFlag = tossimFlag;
		this.debugLeds = false;
	}

	/**
     * The configuration which the component is in is
     * null if this the outermost configuration
     * 
     */
    protected NesCConfiguration configuration;

    /**
     * The site on which this component runs
     */
    protected Site site;

    /**
     * Records whether this is a generic component in the nesC sense (i.e., has been instantiated)
     */
    protected boolean instanceOfGeneric;

    /**
     * Return the component name according to naming rules
     */
    @Override
    abstract public String toString();

    /**
     * Produces a nesC file based on the parameters set for this component
     * @throws CodeGenerationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     * @throws URISyntaxException 
     */
    public abstract void writeNesCFile(String outputDir) throws CodeGenerationException;

    /**
     * Returns a string used to declare this component in a configuration file  
     */
    public String getDeclaration() {
	return "\tcomponents " + this.id + ";\n";
    }

    public boolean isInstanceOfGeneric() {
	return this.instanceOfGeneric;
    }
    
    /**
     * Writes a nesC file with the given name
     * @param templateName
     * @param destFName
     * @param replacements
     * @throws IOException
     * @throws URISyntaxException 
     */
    protected void writeNesCFile(final String templateName,
	    final String destFName, final HashMap<String, String> replacements)
	    throws IOException, CodeGenerationException, URISyntaxException {
    	
    	replacements.put("__MODULE_NAME__", this.getID());
    	replacements.put("__DBG_CHANNEL__", "DBG_"+this.getID().toUpperCase());
    	replacements.put("__HEADER__", this.configuration
    			.generateModuleHeader(this.getID()));

    	Template.instantiate(templateName, destFName, replacements);
    }

    public void writeNesCFile(final String sourceFName,
	    final String destFName) throws IOException, CodeGenerationException, URISyntaxException {
	writeNesCFile(sourceFName, destFName, new HashMap<String, String>());
    }

    public static String generateNesCMethods(final String templateName,
	    final HashMap<String, String> replacements) throws IOException {

	final File inFile = new File(templateName);
	final BufferedReader in = new BufferedReader(new FileReader(inFile));
	final StringBuffer outBuff = new StringBuffer();

	String line;
	while ((line = in.readLine()) != null) {
	    final Iterator<String> replaceIter = replacements.keySet()
		    .iterator();
	    while (replaceIter.hasNext()) {
		final String replaceText = replaceIter.next();

		if (line.contains(replaceText)) {
		    final String replaceWith = replacements.get(replaceText);
		    line = line.replace(replaceText, replaceWith);
		}
	    }

	    outBuff.append(line + "\n");
	}

	return outBuff.toString();
    }

    public static String generateNesCOutputFileName(final String outputDir,
	    final String sourceFile) throws IOException {

    	return outputDir + sourceFile + ".nc";
    }

    public String getDescription() {
	return this.description;
    }

    public void setDescription(final String desc) {
	this.description = desc;
    }

    public Site getSite() {
	return this.site;
    }
    
    public boolean isSystemComponent() {
    	return this.systemComponent;
    }
}
