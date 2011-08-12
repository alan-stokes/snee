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

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

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

	/**
	 * Constructor for LAF.
	 * @param rootOp
	 * @param queryName
	 */
	public LAF(LogicalOperator rootOp, String queryName) {
		super(queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER LAF() " + queryName);
		this.logicalOperatorTree = new Tree(rootOp);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN LAF()");
		}
	}

	/**
	 * Resets the candidate counter; use prior to compiling the next query.
	 */
	public static void resetCandidateCounter() {
		candidateCount = 0;
	}

	 /** {@inheritDoc} */
	public String generateID(String queryName) {
//		if (logger.isDebugEnabled())
//			logger.debug("ENTER generateID()");
		candidateCount++;
//		if (logger.isDebugEnabled())
//			logger.debug("RETURN generateID()");
		return queryName + "-LAF-" + candidateCount;
	}

	/**
	 * Returns the root operator in the tree.
	 * @return the root operator.
	 */
	public LogicalOperator getRootOperator() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getRootOperator()");
		LogicalOperator rootOp = 
			(LogicalOperator) this.logicalOperatorTree.getRoot();
		if (logger.isDebugEnabled())
			logger.debug("RETURN getRootOperator() with " + rootOp);
		return rootOp;
	}

	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()");
		String id = this.getID();
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString() id=" + id);
		return id;
	}

	/**
	 * Removes an operator from the operator tree.
	 * @param op
	 * @throws OptimizationException
	 */
	public void removeOperator(LogicalOperator op) 
	throws OptimizationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER removeOperator()");
		this.logicalOperatorTree.removeNode(op);
		if (logger.isDebugEnabled())
			logger.debug("RETURN removeOperator()");
	}

	/**
	 * Creates an iterator to traverse the operator tree.
	 * @param order
	 * @return
	 */
	public Iterator<LogicalOperator> operatorIterator(TraversalOrder order) {
//		if (logger.isDebugEnabled())
//			logger.debug("ENTER operatorIterator()");
//		if (logger.isDebugEnabled())
//			logger.debug("RETURN operatorIterator()");		
		return this.logicalOperatorTree.nodeIterator(order);
	}

	/**
	 * Gets the operator tree.
	 * @return
	 */
	public Tree getOperatorTree() {
//		if (logger.isDebugEnabled())
//			logger.debug("ENTER getOperatorTree()");
//		if (logger.isDebugEnabled())
//			logger.debug("RETURN getOperatorTree()");	
		return this.logicalOperatorTree;
	}
	
}
