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
    public static final String GENERATE_QEP_IMAGES = 
    	"compiler.generate_graphs";

    /**
     * Used to indicate if graphs will be converted to png
     * Optional, requires valid graphviz location
     */
    public static final String CONVERT_QEP_IMAGES = 
    	"compiler.convert_graphs";

    /**
     * Used to indicate whether operator output types should
     * be displayed in the operator trees.
     */
    public static final String SHOW_OPERATOR_TUPLE_TYPE = 
    	"compiler.debug.show_operator_tuple_type";
    
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
     * 
     */
	public static final String ALGORITHM_SELECTOR_ENABLE_INCREMENTAL_AGGREGATION = 
		"compiler.algorithmselector.enable_incremental_aggregation";
    
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

    /** Specifies whether agendas generated should allow sensing to have 
     * interruptions. Use this option to enable high acquisition intervals.
     */
    public static final String ALLOW_DISCONTINUOUS_SENSING =
    	"compiler.allow_discontinuous_sensing";    	
    
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
	
	public static String RESULTS_HISTORY_SIZE_TUPLES =
		"results.history_size.tuples";

	/**
	 * Specifies whether the metadata collection program should be invoked, 
	 * or default metadata should be used.
	 */
	public static String SNCB_PERFORM_METADATA_COLLECTION = 
		"sncb.perform_metadata_collection";
	
	/**
	 * Specifies whether the command server should be included with SNEE 
	 * query plan
	 */
	public static String SNCB_INCLUDE_COMMAND_SERVER =
		"sncb.include_command_server";
	
	/**
	 * Flag to determine whether individual images or a single image is sent
	 * to WSN nodes.
	 */
	public static String SNCB_GENERATE_COMBINED_IMAGE = 
		"sncb.generate_combined_image";

	/** 
	 * Code generation target for code generator
	 */
	public static final String SNCB_CODE_GENERATION_TARGET = 
		"sncb.code_generation_target";

	/**
	 * Turns the radio on/off during agenda evalauation, 
	 * to enable power management to kick in.
	 * Ignored for tossim and telosb targets.
	 */
	public static final String SNCB_CONTROL_RADIO = "sncb.control_radio";
	
	/**
	 * strategies to be used within the wsn manager for node failure(FL,FP, FG, ALL)
	 */
	public static final String WSN_MANAGER_STRATEGIES = "wsn_manager.strategies";
	
	/**
	 * used by the failed node local strategy to enforce a minimum resilience level
	 */
	public static final String WSN_MANAGER_K_RESILENCE_LEVEL = "wsn_manager.k_resilence_level";
	
	/**
	 * used by the failed node local strategy to determine if sensing nodes are counted within the k 
	 * resilience calculation
	 */
	 public static final String WSN_MANAGER_K_RESILENCE_SENSE = "wsn_manager.k_resilence_sense";
	 
	 /**
	  * used by the planning phase of the manager to determine if 
	  * the successor relation should be generated
	  */
	 public static final String WSN_MANAGER_SUCCESSOR = "wsn_manager.successor";
	
	 /**
    * used by the planning phase of the manager to determine if 
    * the successor relation should be generated
    */
   public static final String WSN_MANAGER_INITILISE_FRAMEWORKS = "wsn_manager.setup_frameworks";
	 
	/**
	 * choice preference for the assessor for which type of adaptation to choose 
	 * (global, partial, local and best)
	 */
	public static final String CHOICE_ASSESSOR_PREFERENCE = "choiceAssessorPreference";
	
	/**
	 * used to determine if the autonomic manager is to run the cost models before normal execution
	 */
	public static final String RUN_COST_MODELS = "runCostModel";
	
	/**
	 * used to determine if the autonomic manager is to run the failed nodes before normal execution
	 */
	public static final String RUN_SIM_FAILED_NODES= "runSimFailedNodes";
	
	/**
	 * used to determine if the autonomic manager is to run the cost models before normal execution
	 */
	public static final String RUN_NODES_WITH_FAILURES = "runWithFailures";
	
	/**
	 * used to decide if avrora is ran in real time or super fast
	 */
	public static final String AVRORA_REAL_TIME = "avroraRealTime";

	/**
	 * used by the batch processing to stop avrora running
	 */
  public static final String RUN_AVRORA_SIMULATOR = "runAvrora";

  /**
   * used by the autonomic managers planner to determine which successor relation to run
   */
  public static final String WSN_MANAGER_PLANNER_OVERLAYSUCCESSOR = "runOverlaySuccessor";
  
  /**
   * used by the autonomic managers planner to determine which successor relation to run
   */
  public static final String WSN_MANAGER_PLANNER_SUCCESSOR = "runSuccessor";
  
	
}



