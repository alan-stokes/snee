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
package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class LAF extends SNEEAlgebraicForm {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(LAF.class.getName());

	/**
	 * Counter used to assign unique id to different candidates.
	 */
	protected static int candidateCount = 0;
	
	/**
	 * The logical operator tree.
	 */	
	private Tree logicalOperatorTree;


	/** Acquisition interval of the whole query. (Alpha)*/
	private double acInt;

	/**
	 * Main construction used by logical optimizer.
	 * @param inRootOp The root operator of the logical query plan
	 * @param queryName The name of the query
	 * @param acquisitionInterval Acquisition interval of the whole query.
	 *  (Alpha)
	 */
	public LAF(LogicalOperator rootOp, String queryName){//,
//			long acquisitionInterval) {
		super(queryName);
		this.logicalOperatorTree = new Tree(rootOp);
//		this.setAcquisitionInterval(acquisitionInterval);
	}

	/**
	 * Resets the candidate counter; use prior to compiling the next query.
	 */
	public static void resetCandidateCounter() {
		candidateCount = 0;
	}

	/**
	 * Generates a systematic name for this query plan structure, 
	 * of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
	public String generateName(String queryName) {
		candidateCount++;
		return queryName + "-LAF-" + candidateCount;
	}

	/**
	 * Returns the root operator in the tree.
	 * @return the root operator.
	 */
	public LogicalOperator getRootOperator() {
		return (LogicalOperator) this.logicalOperatorTree.getRoot();
	}

	/**
	 * Sets the acquisition interval for the Plan 
	 * 		and the operators where required.
	 * @param acquisitionInterval Acquisition interval of the whole query. 
	 * (Alpha)
	 */
	public void setAcquisitionInterval(double acquisitionInterval) {
		this.acInt = acquisitionInterval;
		Iterator<Node> opIter = this.logicalOperatorTree.
			nodeIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = (LogicalOperator) opIter.next();
			if (op instanceof WindowOperator) {
				((WindowOperator) op).setAcquisitionInterval(acInt);
			}
		}
	}

	/**
	 * Gets the acquisition interval for the Plan 
	 * 		and the operators where required.
	 * @return acquisitionInterval Acquisition interval of the whole query. 
	 * (Alpha)
	 */
	public double getAcquisitionInterval() {
		return this.acInt;
	}

	public String getProvenanceString() {
		return this.getName();
	}

	public void removeOperator(LogicalOperator op) throws OptimizationException {
		this.logicalOperatorTree.removeNode(op);
	}

	public Iterator<LogicalOperator> operatorIterator(TraversalOrder order) {
		return this.logicalOperatorTree.nodeIterator(order);
	}

	public Tree getOperatorTree() {
		return this.logicalOperatorTree;
	}
}
