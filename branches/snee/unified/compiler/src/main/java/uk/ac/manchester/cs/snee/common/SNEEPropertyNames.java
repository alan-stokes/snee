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
package uk.ac.manchester.cs.snee.common;

/**
 * Property names used by SNEE.
 */
public final class SNEEPropertyNames {

    /**
     * Used to indicate if graphs will be generated
     * Optional
     */
    public static final String GENERAL_GENERATE_GRAPHS = 
    	"compiler.generate_graphs";

    /**
     * Used to provide the path to the GraphViz executatble
     * Optional, must be set if GENERAL_GENERATE_GRAPHS=true 
     */
    public static final String GRAPHVIZ_EXE =
    	"graphviz.exe";
    
    /**
     * Indicates whether old output files will be deleted.
     * Optional
     */
    public static final String GENERAL_DELETE_OLD_FILES =
    	"delete_old_files";
    
    /**
     * Output files root directory.
     */
    public static final String GENERAL_OUTPUT_ROOT_DIR =
    	"compiler.output_root_dir";
    
    /**
	 * The name of the file with the logical schema.
	 * Optional
	 */
	public static String INPUTS_SCHEMA_FILE =
		"logical_schema";
	
    /**
	 * The name of the file with the physical schema.
	 * Optional
	 */
	public static String INPUTS_PHYSICAL_SCHEMA_FILE =
		"physical_schema";
	
	/**
	 * The name of the file with the operator metadata.
	 * Optional
	 */
	public static String INPUTS_COST_PARAMETERS =
		"cost_parameters";

	/**
	 * The name of the file with the type definitions.
	 */
	public static String INPUTS_TYPES_FILE =
		"types_file";

	/**
	 * The name of the file with the user unit definitions.
	 */	
	public static String INPUTS_UNITS_FILE =
		"units_file";

}



