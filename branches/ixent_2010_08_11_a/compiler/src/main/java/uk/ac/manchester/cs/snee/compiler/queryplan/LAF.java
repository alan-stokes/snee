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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Graph;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class LAF extends Graph {
	
	/**
	 * The root of the operator tree.
	 */
	protected LogicalOperator rootOp;

	/**
	 *  Set of leaf operators in the query plan.
	 */
	private HashSet<LogicalOperator> leafOperators = 
		new HashSet<LogicalOperator>();

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(LAF.class.getName());

	/**
	 * Counter used to assign unique id to different candidates.
	 */
	protected static int candidateCount = 0;

	/**
	 * Implicit constructor used by subclass.
	 */
	protected LAF() { }    

	/** Acquisition interval of the whole query. (Alpha)*/
	private double acInt;

	private String queryName;

	/**
	 * Main construction used by logical optimizer.
	 * @param inRootOp The root operator of the logical query plan
	 * @param queryName The name of the query
	 * @param acquisitionInterval Acquisition interval of the whole query.
	 *  (Alpha)
	 */
	public LAF(LogicalOperator inRootOp, String queryName){//,
//			long acquisitionInterval) {
		this.name = generateName(queryName);
		this.queryName=queryName;
		this.rootOp = inRootOp;
		this.updateNodesAndEdgesColls(this.rootOp);
//		this.setAcquisitionInterval(acquisitionInterval);
	}

	/**
	 * Constructor used by clone.
	 * @param laf The LAF to be cloned
	 * @param inName The name to be assigned to the new data structure.
	 */
	public LAF(LAF laf, String inName) {
		super(laf, inName);

		rootOp = (LogicalOperator) nodes.get(laf.rootOp.getID());

		Iterator<LogicalOperator> opIter = laf.leafOperators.iterator();
		while (opIter.hasNext()) {
			String opID = opIter.next().getID();
			this.leafOperators.add((LogicalOperator) nodes.get(opID));
		}

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
	private static String generateName(String queryName) {
		candidateCount++;
		return queryName + "-LAF-" + candidateCount;
	}

	/**
	 * Updates the nodes and edges collections according to the tree 
	 * passed to it.
	 * @param op The current operator being processed
	 */
	public void updateNodesAndEdgesColls(LogicalOperator op) {
		this.nodes.put(op.getID(), op);

		/* Post-order traversal of operator tree */
		if (!op.isLeaf()) {
			for (int childIndex = 0; childIndex < op.getInDegree(); 
			childIndex++) {
				LogicalOperator c = (LogicalOperator) op.getInput(childIndex);

				this.updateNodesAndEdgesColls(c);
				EdgeImplementation e = new EdgeImplementation(this
						.generateEdgeID(c.getID(), op.getID()), c.getID(), op
						.getID());
				this.edges.put(this.generateEdgeID(c.getID(), op.getID()), e);
			}
		}
	}

	/**
	 * Returns the root operator in the tree.
	 * @return the root operator.
	 */
	public LogicalOperator getRootOperator() {
		return this.rootOp;
	}

	/**
	 * Helper method to recursively generate the operator iterator.
	 * @param op the operator being visited
	 * @param opList the operator list being created
	 * @param traversalOrder the traversal order desired 
	 */
	private void doOperatorIterator(LogicalOperator op,
			ArrayList<LogicalOperator> opList, 
			TraversalOrder traversalOrder) {

		if (traversalOrder == TraversalOrder.PRE_ORDER) {
			opList.add(op);
		}

		for (int n = 0; n < op.getInDegree(); n++) {
			this.doOperatorIterator(op.getInput(n), opList, traversalOrder);
		}

		if (traversalOrder == TraversalOrder.POST_ORDER) {
			opList.add(op);
		}
	}	

	/**
	 * Iterator to traverse the operator tree.
	 * The structure of the operator tree may not be modified during 
	 * iteration
	 * @param traversalOrder the order to traverse the operator tree
	 * @return an iterator for the operator tree
	 */
	public Iterator<LogicalOperator> operatorIterator(
			TraversalOrder traversalOrder) {

		ArrayList<LogicalOperator> opList = 
			new ArrayList<LogicalOperator>();
		this.doOperatorIterator(this.getRootOperator(), opList, 
				traversalOrder);

		return opList.iterator();
	}

	/**
	 * Sets the acquisition interval for the Plan 
	 * 		and the operators where required.
	 * @param acquisitionInterval Acquisition interval of the whole query. 
	 * (Alpha)
	 */
	public void setAcquisitionInterval(double acquisitionInterval) {
		this.acInt = acquisitionInterval;
		Iterator<LogicalOperator> opIter = 
			operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
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
		return this.name;
	}

	public String getQueryName() {
		return queryName;
	}

	public void setName(String newLafName) {
		this.name = newLafName;
	}
}
