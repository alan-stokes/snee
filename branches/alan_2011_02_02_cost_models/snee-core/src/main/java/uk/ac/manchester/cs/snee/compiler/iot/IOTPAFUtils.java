package uk.ac.manchester.cs.snee.compiler.iot;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * Utility class for displaying PAF.
 */
public class IOTPAFUtils extends DLAFUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(IOTPAFUtils.class.getName());
	
	/**
	 * PAF to be displayed.
	 */
	private PAF paf;
	
	/**
	 * Constructor for LAFUtils.
	 * @param laf
	 */	
	public IOTPAFUtils(PAF paf) {
		super(paf.getDLAF());
		if (logger.isDebugEnabled())
			logger.debug("ENTER PAFUtils()"); 
		this.paf = paf;
		this.name = paf.getID();
		this.tree = paf.getOperatorTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN PAFUtils()"); 
	}
	
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
			logger.debug("ENTER exportAsDOTFile() with file:" + 
					fname + "\tlabel: " + label);
		}
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(fname)));

			out.println("digraph \"" + this.name + "\" {");
			String edgeSymbol = "->";

			//query plan root at the top
			out.println("size = \"8.5,11\";"); // do not exceed size A4
			out.println("rankdir=\"BT\";");
			out.println("label=\"" + this.name //was laf.getProvenanceString 
					+ label + "\";");

			//Draw fragments info; will be empty for LAF and PAF
			out.println(fragmentsBuff.toString());

			/**
			 * Draw the nodes, and their properties
			 */
		//	Iterator<Node> opIter = 
			//	tree.nodeIterator(TraversalOrder.POST_ORDER);
			Iterator<Node> opIter = paf.getOperatorTree().getNodes().iterator();
			while (opIter.hasNext()) {
				InstanceOperator op = (InstanceOperator) opIter.next();
				out.print("\"" + op.getID() + "\" [fontsize=9 ");

				if (op instanceof InstanceExchangePart) {
					out.print("fontcolor = blue ");
				}

				out.print("label = \"");
				
				if ((showOperatorCollectionType) && !(op instanceof InstanceExchangePart)) {
					out.print("(" + op.toString()
							+ ") ");
				}
				out.print(op.getID() + "\\n");

				if (showOperatorID) {
					out.print("id = " + op.getID() + "\\n");
				}
					
				//print subclass attributes
				if (opLabelBuff.get(op.getID()) != null) {
					out.print(opLabelBuff.get(op.getID())); 
				}
				out.println("\" ]; ");
			}

			/**
			 * Draw the edges, and their properties
			 */
			opIter = paf.getOperatorTree().getNodes().iterator();
			ArrayList<EdgeImplementation> listOfEdges = new ArrayList<EdgeImplementation>();
			while (opIter.hasNext()) {
				InstanceOperator op = (InstanceOperator) opIter.next();
				Iterator<EdgeImplementation> edgeIter = paf.getOperatorTree().getNodeEdges(op.getID()).iterator();
				while (edgeIter.hasNext()) {
				  EdgeImplementation edge = edgeIter.next();
				  if(!listOfEdges.contains(edge))
				  {
				    listOfEdges.add(edge);
					out.print("\"" + edge.getSourceID() + "\"" + edgeSymbol + "\""
							+ edge.getDestID() + "\" ");				
					out.print("[fontsize=9 label = \" ");
					/*try {
						if (showTupleTypes && (!(childOp instanceof SensornetExchangeOperator))) {
							out.print("type: " + 
								childOp.getTupleAttributesStr(3) + " \\n");
						}
					} catch (TypeMappingException e1) {
						String msg = "Problem getting tuple attributes. " + e1;
						logger.warn(msg);
					}*/
					//print subclass attributes
//TODO: Not sure what this is for					
//					if (edgeLabelBuff.get(e.getID()) != null) {
//						out.print(edgeLabelBuff.get(e.getID()));
//					}
					out.print("\"];\n");
				  }
				}
			}
			out.println("}");
			out.close();
		} catch (Exception e) {
			logger.warn("Failed to write PAF to " + fname + ".", e);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN exportAsDOTFile()");
		}
	}
}
