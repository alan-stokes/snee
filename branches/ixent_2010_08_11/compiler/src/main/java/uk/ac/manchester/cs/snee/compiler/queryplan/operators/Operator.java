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
package uk.ac.manchester.cs.snee.compiler.queryplan.operators;

import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
//import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
//import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
//import uk.ac.manchester.cs.snee.compiler.whenScheduling.qosaware.cvx.AlphaBetaExpression;

/**
 * Base class for all operators. Specific operators
 * extend this class. 
 * @author Ixent Galpin ,Christian Brenninkmeijer and Steven Lynden 
 */
public interface Operator extends Node {
	//TODO: Change to logical operators

	//     /** @return the fragment to which this operator belongs. */
	//    Fragment getContainingFragment();
	//        
	//    /** @return ID of the containing Fragment. */
	//    String getFragID();
	//    
	//    /** @return True if this operator is the root of its fragment, */
	//    boolean isFragmentRoot();

	//CB: Used by the TreeDisplayer
	//IG: TODO: Ugly for now, but hopefully change return type to List
	/** @return Array of the child nodes. */
	Node[] getInputs();
	//public Operator[] getInputs();

	//CB: Used by logicalOptimiser
	/** 
	 * Returns The child operator at the given index.
	 * @param index The location of the required child operator.
	 * @return The child operator at the index specified.
	 */
	Operator getInput(int index);

	/** 
	 * Returns The parent operator at the given index.
	 * @param index The location of the required child operator.
	 * @return The parent operator at the index specified.
	 */
	Operator getOutput(int index);

	//    /** @return The attribute(s) by which data is partitioned. */
	//    String getPartitioningAttribute();

	/** {@inheritDoc} */
	String toString();

//	//CB: Used by TreeDisplay and toString for debugging
//	//CB: IXENT feel free to add output here
//	//CB: Should not children use to string for that.
//	/**
//	 * Returns text description of this operator.
//	 * @param showProperties If True returns more information.
//	 * @return A String representation of this operator.
//	 */
//	String getText(boolean showProperties);

	/**
	 * Returns text description of this operator.
	 * @return A String representation of this operator.
	 */
	String getText();

	/** 
	 * Retreives the parent operator.
	 * @return The parent operator.
	 */
	Operator getParent();

	//    /**
	//     * Sets the containing fragment of this operator.
	//     * @param f the containing fragment
	//     */
	//    void setContainingFragment(Fragment f);

	/**
	 * Iterator to traverse the immediate children of the current operator.
	 * @return Iterator through the child operators.
	 */
	Iterator<Operator> childOperatorIterator();

	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */ 
	List<Attribute> getAttributes();

	/**
	 * Gets the expressions that describe the data returned by this operator.
	 * @return List of expressions.
	 */ 
	List<Expression> getExpressions();

	/**
	 * String description of the attributes.
	 * @param maxPerLine Number of attributes per line.
	 * @return A multiline string representation of the attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	String getTupleAttributesStr(int maxPerLine) 
	throws SchemaMetadataException, TypeMappingException;

//	/**
//	 * Get the source node for the operator by getting the source nodes of
//	 * it children.
//	 * 
//	 * @return Sorted list of nodes that provide data for this operator.
//	 */
//	int[] getSourceSites();

	/**
	 * Calculated the cardinality based on the requested type. 
	 * For exchanges this is the producer cardinality
	 * 
	 * @param card Type of cardinailty to be considered.
	 * 
	 * @return The Cardinality calulated as requested.
	 */
	int getCardinality(CardinalityType card);

	//    /**
	//     * The size of the output.
	//     * This size considers distribution of the query plan.
	//     * It is the output for a specific SensorNetworkNode
	//     * 
	//     * For exchanges this is the producer cardinality
	//     * @param card CardinalityType The type of cardinality to be considered.
	//     * @param node Physical mote on which this operator has been placed.
	//     * @param daf Distributed query plan this operator is part of.
	//     * @return Sum of the cardinality of the iall nputs 
	//     * for this operator on this node.
	//     */
	//	int getCardinality(CardinalityType card, Site node, DAF daf);

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
	 * Used to determine if the operator is Attribute sensitive.
	 * 
	 * @return true only if operator is attribute sensitive.
	 */
	boolean isAttributeSensitive(); 

	/**
	 * Detects if operator must be based on particular sites.
	 * @return True if and only if operator requires specific sites.
	 */
	boolean isLocationSensitive();

	/**
	 * Detects if operator can call itself.
	 * @return True if and only if operator is recursive.
	 */
	boolean isRecursive();

	/**
	 * Detects if operator allows predicates pushed into it.
	 * Allows the pushing of select into previous operator.
	 * @return True if both the operator and the settings allow it.
	 */
	boolean acceptsPredicates();

	/**
	 * Sets predicates on this operator.
	 * @param newPredicate Predicate to set or replace existing predicate.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	void setPredicate(Expression newPredicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException;

//	/**
//	 * Creates a copy of this Operator.
//	 * Does not create a new instances of any internal values.
//	 * @return A copy of this operator 
//	 * 	which shares all internal data structures. 
//	 */
//	Operator shallowClone();

//	/**
//	 * Gets a String to be used by nesc as the name of this operator.
//	 * @return Name to be used as template name.
//	 */
//	String getNesCTemplateName();

	/**
	 * Get the collection type of the data returned.
	 * @return Stream/ window or relation.
	 */
	OperatorDataType getOperatorDataType();

	/**
	 * Gets the name of this operator.
	 * @return The name of the operator.
	 */
	String getOperatorName();

	/** 
	 * Retrieves extra information for this operator.
	 * @return The extra information as a String.
	 */
	String getParamStr();

	/**
	 * This method is will be called from the deliver down to the leaf operator.
	 * It will help operators identify which attributes 
	 * they need to included and in which order. 
	 * 
	 * @param projectExpressions 
	 *    List of expressions used to create the attributes.
	 * @param projectAttributes 
	 *    List of the attribute names to assign to the output.
	 * @return 
	 *   True if and only if the child accepted the pushed down projection. 
	 * 
	 * @throws OptimizationException 
	 *  An exception is any attribute in the list is not in the source; 
	 */
	boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException;

	/**
	 * Allow pushing down of a select Predicate.
	 * 
	 * This method is called from the root down.
	 * If the operator is able to accept the predicate it returns true.
	 * 
	 * @param predicate to be pushed down.
	 * 
	 * @return True if and only if the operator (or its children) 
	 *    are able to accept the predicate.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException;

	/** 
	 * This method will be called by the rename operators.
	 * 
	 * The new localName is pushed down. 
	 * @param newLocalName LocalName to push down.
	 */
	void pushLocalNameDown(String newLocalName);

	/** 
	 * Retrieve the predicate this operator will check.
	 * @return The Predicate (which may be NOPredicate)
	 */ 
	Expression getPredicate();

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

//	/** 
//	 * The size of the whole queue that must be used for the output.
//	 * This may be larger than the maximum number of tuples 
//	 * that the next operator will have to consider.
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
//	 * @param node Site for which the data is required.
//	 * @param daf Required to access which sites are children to this site. 
//	 * 
//	 * @return Usually PhysicalMaxCardinality
//	 */
//	int getOutputQueueCardinality(Site node, DAF daf);

//	/**
//	 * The physical size of the tuple including any control information.
//	 * 
//	 * @return the logical tuple size unless control information is added.
//	 */
//	int getPhysicalTupleSize();

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

//	/** 
//	 * Calculates the physical size of the state of this operator.
//	 * 
//	 * Does not included the size of the input 
//	 * as these are assumed passed by reference.
//	 *
//	 * Does not include the size of the code itself.
//	 * 
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	int getDataMemoryCost(Site node, DAF daf);

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int getDataMemoryCost2(Site node, DAF daf);

//	/**
//	 * Calculates the time cost for a single evaluation of this operator.
//	 * 
//	 * Includes the time to call the child and to create the event reply.
//	 * Does not include the cost of any child operators.
//	 * 	  
//	 * Based on the time estimates provided in the OperatorsMetaData file.
//	 * 
//	 * @param card Type of Cardinality to be used to calculate cost.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	double getTimeCost(CardinalityType card, Site node, DAF daf);

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

	/**
	 * Some operators do not change the data in any way.
	 * these can be removed. 
	 * 
	 * @return True If and only if the operator can be safely removed. 
	 */
	boolean isRemoveable();

}
