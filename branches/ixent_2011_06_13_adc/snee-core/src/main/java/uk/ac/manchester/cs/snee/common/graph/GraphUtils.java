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

package uk.ac.manchester.cs.snee.common.graph;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;


/**
 * @author Ixent Galpin
 * 
 * Graph Display operations.
 *   
 */

public class GraphUtils {

    private static Logger logger = Logger.getLogger(GraphUtils.class
	    .getName());

    /**
     * Converts a file in the DOT, the Graphviz graph specification 
     * language into a PNG image.
     * @param inputFullPath	the DOT file
     * @param outputFullPath the PNG image
     */
    private static void convertDOT2PNG(final String inputFullPath,
	    final String outputFullPath) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER convertDOT2PNG()");
    	
		//Code adapted from	
		//http://www.javaworld.com/javaworld/jw-12-2000/jw-1229-traps.html?page=3
		try {
		    Runtime rt = Runtime.getRuntime();
		    String graphvizExe = 
		    	SNEEProperties.getSetting(SNEEPropertyNames.GRAPHVIZ_EXE);
		    String[] commandStr = new String[] {graphvizExe, "-Tpng", "-o"+ 
		    	outputFullPath, inputFullPath};
		    logger.trace(graphvizExe + " -Tpng" + " -o"+ 
			    	outputFullPath + " " + inputFullPath);
		    Process proc = rt.exec(commandStr);
		    InputStream stderr = proc.getErrorStream();
		    InputStreamReader isr = new InputStreamReader(stderr);
		    BufferedReader br = new BufferedReader(isr);
		    String line = null;
		    while ((line = br.readLine()) != null) { }
		    int exitVal = proc.waitFor();
		    logger.trace("Dotfile to PNG process exitValue: " + exitVal);
		} catch (Exception e) {
		    logger.warn("Problem producing PNG image: ", e);
		}
		
		if (logger.isTraceEnabled())
			logger.trace("RETURN convertDOT2PNG()");
    }
    
    public static void generateGraphImage(String dotFilePath) 
    throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER generateGraphImage()");
		if (SNEEProperties.getBoolSetting(
				SNEEPropertyNames.CONVERT_QEP_IMAGES)) {
			if (!dotFilePath.endsWith(".dot")) {
				dotFilePath = dotFilePath + ".dot";
			}
			String pngFilePath = dotFilePath.replaceAll("dot", "png");
			convertDOT2PNG(dotFilePath, pngFilePath);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN generateGraphImage()");
    }

    
}
