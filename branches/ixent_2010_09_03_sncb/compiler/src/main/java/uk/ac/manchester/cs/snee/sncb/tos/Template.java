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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;

public class Template {

    /**
     * Given a template file, replaces parts of text according to the 
     * replacements hashmap, to the destination file. 
     * @param templateName
     * @param destFName
     * @param replacements
     * @throws IOException
     * @throws URISyntaxException 
     */
    public static void instantiate(final String templateName,
	    final String destFName, final HashMap<String, String> replacements)
	    throws IOException, URISyntaxException {

		URL inFileURL = Template.class.getClassLoader().getResource(templateName);
		System.out.println(inFileURL.getFile());
		File inFile = new File(inFileURL.getFile());
		File outFile = new File(destFName);
		
		final BufferedReader in = new BufferedReader(new FileReader(inFile));
		final PrintWriter out = new PrintWriter(new BufferedWriter(
			new FileWriter(outFile)));
	
		String line;
		while ((line = in.readLine()) != null) {
		    //match and replace as necessary
		    final Iterator<String> replaceIter = replacements.keySet()
			    .iterator();
		    while (replaceIter.hasNext()) {
			final String replaceText = replaceIter.next();
	
			if (line.contains(replaceText)) {
			    final String replaceWith = replacements.get(replaceText);
			    line = line.replace(replaceText, replaceWith);
			}
		    }
	
		    out.println(line);	
		}
	
		in.close();
		out.close();
    }

    
    /**
     * Generates a file without any replacements.
     * @param sourceFName
     * @param destFName
     * @throws IOException
     * @throws URISyntaxException 
     */
    public static void instantiate(final String sourceFName,
	    final String destFName) throws IOException, URISyntaxException {
    	instantiate(sourceFName, destFName,
		new HashMap<String, String>());
    }

	
}
