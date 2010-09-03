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
package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public interface SensornetOperator extends Node {
	
     /** @return the fragment to which this operator belongs. */
	Fragment getContainingFragment();
        
    /** @return ID of the containing Fragment. */
    String getFragID();
    
    /** @return True if this operator is the root of its fragment, */
    boolean isFragmentRoot();

    /**
     * Sets the containing fragment of this operator.
     * @param f the containing fragment
     */
	void setContainingFragment(Fragment f);

	/**
	 * Get the source node for the operator by getting the source nodes of
	 * it children.
	 * 
	 * @return Sorted list of nodes that provide data for this operator.
	 */
	int[] getSourceSites();

    /**
     * The size of the output.
     * This size considers distribution of the query plan.
     * It is the output for a specific SensorNetworkNode
     * 
     * For exchanges this is the producer cardinality
     * @param card CardinalityType The type of cardinality to be considered.
     * @param node Physical mote on which this operator has been placed.
     * @param daf Distributed query plan this operator is part of.
     * @return Sum of the cardinality of the iall nputs 
     * for this operator on this node.
     * @throws OptimizationException 
     */
    int getCardinality(CardinalityType card, Site node, DAF daf) 
    throws OptimizationException;

//	/**
//     * The size of the output.
//     * This size considers distribution of the query plan.
//     * It is the output for a specific SensorNetworkNode
//     * 
//     * For exchanges this is the producer cardinality
//     * @param card CardinalityType The type of cardinality to be considered.
//     * @param node Physical mote on which this operator has been placed.
//     * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//     * @return Sum of the cardinality of the iall nputs 
//     * for this operator on this node.
//     */
//	AlphaBetaExpression getCardinality(CardinalityType card, Site node, 
//			DAF daf, boolean round);

	/**
	 * Gets a String to be used by nesc as the name of this operator.
	 * @return Name to be used as template name.
	 */
	public String getNesCTemplateName();

	public SensornetOperator getParent();
	
	public LogicalOperator getLogicalOperator();

	public int getCardinality(CardinalityType physicalMax);

	public boolean isAttributeSensitive();
	
	public Iterator<SensornetOperator> childOperatorIterator();
	
	public String getOperatorName();

//	/**
//	 * The physical average size of the output.
//	 * This needs not be the same as the logical.
//	 * 
//	 * @return The logical size.
//	 * /
//	public int getPhysicalAvgCardinality();
//	 */

//	/** 
//	 * The size of the whole queue that must be used for the output.
//	 * This may be larger than the maximum number of tuples 
//	 * that the next operator will have to consider.
//	 * <p>
//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
//	 * <p>
//	 * In most operators this will be the PhysicalMaxCardinality.
//	 * <p> 
//	 * An example of an operator where the queue size may be larger 
//	 * than the maximum numeber of tuple are the window operators.
//	 * The data structure used to hold the tuples will have extra space
//	 * for tuples waiting to enter ther window 
//	 * due to the until or slide factors.
//	 * <p>
//	 * Head and tail will be used to point the next operator 
//	 * to the part of the queue
//	 * relative to this particular window.
//	 * 
//	 * @param numberOfInstances Number of instances if this operator in the query plan.
//	 * Unless numberOfInstance = number of source sites or 1 the correctness 
//	 * 	of this method depends on instances being perfectly distributed.
//	 * @return Usually PhysicalMaxCardinality
//	 */
//	int getOutputQueueCardinality(int numberOfInstances);	

	/** 
	 * The size of the whole queue that must be used for the output.
	 * This may be larger than the maximum number of tuples 
	 * that the next operator will have to consider.
	 * <p>
	 * In most operators this will be the PhysicalMaxCardinality.
	 * <p> 
	 * An example of an operator where the queue size may be larger 
	 * than the maximum numeber of tuple are the window operators.
	 * The data structure used to hold the tuples will have extra space
	 * for tuples waiting to enter ther window 
	 * due to the until or slide factors.
	 * <p>
	 * Head and tail will be used to point the next operator 
	 * to the part of the queue
	 * relative to this particular window.
	 * @param node Site for which the data is required.
	 * @param daf Required to access which sites are children to this site. 
	 * 
	 * @return Usually PhysicalMaxCardinality
	 * @throws OptimizationException 
	 */
	int getOutputQueueCardinality(Site node, DAF daf) throws OptimizationException;

	/**
	 * The physical size of the tuple including any control information.
	 * 
	 * @return the logical tuple size unless control information is added.
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	int getPhysicalTupleSize() throws SchemaMetadataException, TypeMappingException;

//	/** 
//	 * Calculates the physical size of the state of this operator.
//	 * 
//	 * Does not included the size of the input 
//	 * as these are assumed passed by reference.
//	 *
//	 * Does not include the size of the code itself.
//	 * 
//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
//	 * 
//	 * @param numberOfInstances Number of instances if this operator in the query plan.
//	 * Unless numberOfInstance = number of source sites or 1 the correctness 
//	 * 	of this method depends on instances being perfectly distributed.
//	 * @return Estimated number of bytes of RAM used by this operator.
//	 */
//	int getDataMemoryCost(int numberOfInstances);

	/** 
	 * Calculates the physical size of the state of this operator.
	 * 
	 * Does not included the size of the input 
	 * as these are assumed passed by reference.
	 *
	 * Does not include the size of the code itself.
	 * 
	 * @param node Physical mote on which this operator has been placed.
	 * @param daf Distributed query plan this operator is part of.
	 * @return OutputQueueCardinality * PhytsicalTuplesSize
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 */
	int getDataMemoryCost(Site node, DAF daf) throws SchemaMetadataException,
	TypeMappingException, OptimizationException;

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int getDataMemoryCost2(Site node, DAF daf);

	/**
	 * Calculates the time cost for a single evaluation of this operator.
	 * 
	 * Includes the time to call the child and to create the event reply.
	 * Does not include the cost of any child operators.
	 * 	  
	 * Based on the time estimates provided in the OperatorsMetaData file.
	 * 
	 * @param card Type of Cardinality to be used to calculate cost.
	 * @param node Physical mote on which this operator has been placed.
	 * @param daf Distributed query plan this operator is part of.
	 * @return the calculated time
	 * @throws OptimizationException 
	 */
	double getTimeCost(CardinalityType card, Site node, DAF daf) 
	throws OptimizationException;

//	/**
//	 * Calculates the time cost for a single evaluation of this operator.
//	 * 
//	 * Includes the time to call the child and to create the event reply.
//	 * Does not include the cost of any child operators.
//	 * 	  
//	 * Based on the time estimates provided in the OperatorsMetaData file.
//	 * 
//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
//	 * 
//	 * @param numberOfInstances Number of instances if this operator in the query plan.
//	 * @param card Type of Cardinality to be used to calculate cost.
//	 * Unless numberOfInstance = number of source sites or 1 the correctness 
//	 * 	of this method depends on instances being perfectly distributed.
//	 * @return Estimated number of bytes of RAM used by this operator.
//	 */
//	double getTimeCost(CardinalityType card, int numberOfInstances);

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf, long bufferingFactor);

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf);

//	/** 
//	 * Calculates the marginal energy cost of this operator.
//	 * 
//	 * Gives the difference to having the node in sleep state.
//	 * Does not included the energy cost of the input. 
//	 *
//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
//	 * 
//	 * @param numberOfInstances Number of instances if this operator in the query plan.
//	 * @param card Type of Cardinality to be used to calculate cost.
//	 * Unless numberOfInstance = number of source sites or 1 the correctness 
//	 * 	of this method depends on instances being perfectly distributed.
//	 * @return Estimated number of bytes of RAM used by this operator.
//	 */
//	public double getEnergyCost(CardinalityType card, int numberOfInstances);

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf);

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf, long bufferingFactor);

//	/**
//	 * Generates the time cost expressions 
//	 * for a single evaluation of this operator.
//	 * 
//	 * Includes the time to call the child and to create the event reply.
//	 * Does not include the cost of any child operators.
//	 * 
//	 * The time cost is based on the specified cardinality 
//	 * not the average cardinality.
//	 * 
//	 * Based on the time estimates provided in the OperatorsMetaData file.
//	 * 
//	 * @param card Type of Cardinality to be used to calculate cost.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//	 * @return An AlphaBetaExpression of the time this operator will take.
//	 */ 
//	AlphaBetaExpression getTimeExpression(CardinalityType card, Site node, 
//			DAF daf, boolean round);

}
