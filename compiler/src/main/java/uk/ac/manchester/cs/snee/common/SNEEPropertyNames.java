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
    public static final String GENERAL_QEP_IMAGES = 
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
     * Using equivalence-preserving transformation removes unrequired
     * operators (e.g., a NOW window combined with a RSTREAM).
     * TODO: currently in physical rewriter, move this to logical rewriter
     * TODO: consider removing this option
     */
    public static final String LOGICAL_REWRITER_REMOVE_UNREQUIRED_OPS =
    	"compiler.logicalrewriter.remove_unrequired_operators";
    
    /**
     * Pushes project operators as close to the leaves of the operator
     * tree as possible.
     * TODO: currently in physical rewriter, move this to logical rewriter
     * TODO: consider removing this option 
     */
    public static final String LOGICAL_REWRITER_PUSH_PROJECT_DOWN =
    	"compiler.logicalrewriter.push_project_down";
    
    /**
     * Combines leaf operators (receive, acquire, scan) and select 
     * into a single operator.
     * TODO: currently in physical rewriter, move this to logical rewriter
     * TODO: consider removing this option
     */
    public static final String LOGICAL_REWRITER_COMBINE_LEAF_SELECT =
    	"compiler.logicalrewriter.combine_leaf_and_select";
 
    /**
     * Sets the random seed used for generating routing trees.
     */
    public static final String ROUTER_RANDOM_SEED =
    	"compiler.router.random_seed";
    
    /**
     * Removes unnecessary exchange operators from the DAF
     */
    public static final String WHERE_SCHED_REMOVE_REDUNDANT_EXCHANGES =
    	"compiler.where_sched.remove_redundant_exchanges";
    
    /**
     * Instructs where-scheduler to decrease buffering factor
     * to enable a shorter acquisition interval.
     */
    public static final String WHEN_SCHED_DECREASE_BETA_FOR_VALID_ALPHA =
    	"compiler.when_sched.decrease_beta_for_valid_alpha";
    
    /**
	 * The name of the file with the logical schema.
	 * Optional
	 */
	public static String INPUTS_LOGICAL_SCHEMA_FILE =
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
	public static String INPUTS_COST_PARAMETERS_FILE =
		"cost_parameters_file";

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



