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

	//XXX-AG: This method is a conversion from LAF to PAF 
//	/**
//	 * Replaces an operator with another in the routing tree, e.g., 
//	 * used during algorithm selection.
//	 * @param oldOp the operator to be removed
//	 * @param newOp the operator to take its place
//	 */
//	public void replaceNode(Operator oldOp,
//			Operator newOp) {
//		super.replaceNode(oldOp, newOp);
//		if (oldOp == this.rootOp) {
//			this.rootOp = newOp;
//		}
//	}	

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

	//TODO: export methods should be refactored into a support class.
//	public void exportAsDOTFile(String fname) 
//	throws SchemaMetadataException {
//		exportAsDOTFile(fname, new TreeMap<String, StringBuffer>(), 
//				new TreeMap<String, StringBuffer>(), new StringBuffer());
//	}
//
//	public void exportAsDOTFile(String fname, String label) 
//	throws SchemaMetadataException {
//		exportAsDOTFile(fname, new TreeMap<String, StringBuffer>(), 
//				new TreeMap<String, StringBuffer>(), new StringBuffer());
//	}
//
//	protected void exportAsDOTFile(String fname,
//			TreeMap<String, StringBuffer> opLabelBuff,
//			TreeMap<String, StringBuffer> edgeLabelBuff,
//			StringBuffer fragmentsBuff) throws SchemaMetadataException {
//		exportAsDOTFile(fname, "", new TreeMap<String, StringBuffer>(), 
//				new TreeMap<String, StringBuffer>(), new StringBuffer());
//	}

	/**
	 * Exports the graph as a file in the DOT language used by GraphViz.
	 * @see http://www.graphviz.org/
	 *
	 * @param fname the name of the output file
	 * @throws SchemaMetadataException 
	 */
	protected void exportAsDOTFile(String fname,
			String label,
			TreeMap<String, StringBuffer> opLabelBuff,
			TreeMap<String, StringBuffer> edgeLabelBuff,
			StringBuffer fragmentsBuff) 
	throws SchemaMetadataException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER LAF.exportAsDOTFile() with file:" + 
					fname + "\tlabel: " + label);
		}
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(fname)));

			out.println("digraph \"" + (String) this.getName() + "\" {");
			String edgeSymbol = "->";

			//query plan root at the top
			out.println("size = \"8.5,11\";"); // do not exceed size A4
			out.println("rankdir=\"BT\";");
			out.println("label=\"" + this.getProvenanceString() 
					+ label + "\";");

			//Draw fragments info; will be empty for LAF and PAF
			out.println(fragmentsBuff.toString());

			/**
			 * Draw the nodes, and their properties
			 */
			Iterator j = this.nodes.keySet().iterator();
			while (j.hasNext()) {
				LogicalOperator op = (LogicalOperator) this.nodes
				.get((String) j.next());
				out.print("\"" + op.getID() + "\" [fontsize=9 ");

//				if (op instanceof ExchangeOperator) {
//					out.print("fontcolor = blue ");
//				}

				out.print("label = \"");
//				if (Settings.DISPLAY_OPERATOR_DATA_TYPE) {
					out.print("(" + op.getOperatorDataType().toString()
							+ ") ");
//				}
				out.print(op.getOperatorName() + "\\n");

				if (op.getParamStr() != null) {
					out.print(op.getParamStr() + "\\n");
				}
				out.print("id = " + op.getID() + "\\n");

				//print subclass attributes
				if (opLabelBuff.get(op.getID()) != null) {
					out.print(opLabelBuff.get(op.getID())); 
				}
				out.println("\" ]; ");
			}

			/**
			 * Draw the edges, and their properties
			 */
			Iterator i = this.edges.keySet().iterator();
			while (i.hasNext()) {
				Edge e = this.edges.get((String) i.next());
				LogicalOperator sourceNode = (LogicalOperator) this.nodes
				.get(e.getSourceID());

				out.print("\"" + e.getSourceID() + "\"" + edgeSymbol + "\""
						+ e.getDestID() + "\" ");

//				if (Settings.DISPLAY_OPERATOR_PROPERTIES) {
					out.print("[fontsize=9 label = \" ");

					try {
						out.print("type: " + 
								sourceNode.getTupleAttributesStr(3)
								+ " \\n");
					} catch (TypeMappingException e1) {
						String msg = "Problem getting tuple attributes. " + e1;
						logger.warn(msg);
					}

					//print subclass attributes
					if (edgeLabelBuff.get(e.getID()) != null) {
						out.print(edgeLabelBuff.get(e.getID()));
					}

					out.print("\"];\n");
//				} else {
//					out.println(";");
//				}
			}

			out.println("}");
			out.close();
		} catch (IOException e) {
			logger.warn("Failed to write LAF to " + fname + ".");
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN LAF.exportAsDOTFile()");
		}
	}

	public String getProvenanceString() {
		return this.name;
	}
}
