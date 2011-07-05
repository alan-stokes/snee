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
package uk.ac.manchester.cs.snee.operators.logical;

import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
//import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
//import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public interface LogicalOperator extends Node {

	//CB: Used by the TreeDisplayer
	//IG: TODO: Ugly for now, but hopefully change return type to List
	/** @return Array of the child nodes. */
	Node[] getInputs();

	/** 
	 * Returns The child operator at the given index.
	 * @param index The location of the required child operator.
	 * @return The child operator at the index specified.
	 */
	LogicalOperator getInput(int index);

	/** 
	 * Returns The parent operator at the given index.
	 * @param index The location of the required child operator.
	 * @return The parent operator at the index specified.
	 */
	LogicalOperator getOutput(int index);

	/** {@inheritDoc} */
	String toString();

	/**
	 * Returns text description of this operator.
	 * @return A String representation of this operator.
	 */
	String getText();

	/** 
	 * Retrieves the parent operator.
	 * @return The parent operator.
	 */
	LogicalOperator getParent();

	/**
	 * Iterator to traverse the immediate children of the current operator.
	 * @return Iterator through the child operators.
	 */
	Iterator<LogicalOperator> childOperatorIterator();

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

	/**
	 * Calculated the cardinality based on the requested type. 
	 * For exchanges this is the producer cardinality
	 * 
	 * @param card Type of cardinality to be considered.
	 * 
	 * @return The Cardinality calculated as requested.
	 */
	int getCardinality(CardinalityType card);

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

	//XXX: Removed by AG as metadata now handled in metadata object
//	/** 
//	 * This method will be called by the rename operators.
//	 * 
//	 * The new localName is pushed down. 
//	 * @param newLocalName LocalName to push down.
//	 */
//	void pushLocalNameDown(String newLocalName);

	/** 
	 * Retrieve the predicate this operator will check.
	 * @return The Predicate (which may be NOPredicate)
	 */ 
	Expression getPredicate();

	/**
	 * Some operators do not change the data in any way.
	 * these can be removed. 
	 * 
	 * @return True If and only if the operator can be safely removed. 
	 */
	boolean isRemoveable();
	
	/**
	 * 
	 * @return the source type of the operator
	 */
	SourceType getOperatorSourceType();
	
	/**
	 * This method sets the source rate 
	 * for the operator
	 * 
	 * @param rate
	 */
	void setSourceRate(double rate);
	
	/**
	 * 
	 * @return the rate of the operator
	 */
	double getSourceRate();

	/**
	 * Method to set the mode of the operator to get
	 * the next record from child. If it is pull based
	 * then it has to use the getNext() method from child
	 * to get the next data
	 * 
	 * @param isPullBased
	 */
	boolean isGetDataByPullModeOperator();
	
	void setGetDataByPullModeOperator(boolean isPullModeOperator);
}
