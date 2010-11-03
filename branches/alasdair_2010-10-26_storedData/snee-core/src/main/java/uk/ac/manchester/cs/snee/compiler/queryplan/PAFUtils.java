package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * Utility class for displaying PAF.
 */
public class PAFUtils extends DLAFUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(PAFUtils.class.getName());
	
	/**
	 * PAF to be displayed.
	 */
	private PAF paf;
	
	/**
	 * Constructor for LAFUtils.
	 * @param laf
	 */	
	public PAFUtils(PAF paf) {
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
			Iterator<Node> opIter = 
				tree.nodeIterator(TraversalOrder.POST_ORDER);
			while (opIter.hasNext()) {
				SensornetOperator op = (SensornetOperator) opIter.next();
				out.print("\"" + op.getID() + "\" [fontsize=9 ");

				if (op instanceof SensornetExchangeOperator) {
					out.print("fontcolor = blue ");
				}

				out.print("label = \"");
				
				if ((showOperatorCollectionType) && !(op instanceof SensornetExchangeOperator)) {
					out.print("(" + op.getLogicalOperator().getOperatorDataType().toString()
							+ ") ");
				}
				out.print(op.getOperatorName() + "\\n");

				if (!(op instanceof SensornetExchangeOperator)) {
					if (op.getLogicalOperator().getParamStr() != null) {
						out.print(op.getLogicalOperator().getParamStr() + "\\n");
					}
				}
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
			opIter = tree.nodeIterator(TraversalOrder.POST_ORDER);
			while (opIter.hasNext()) {
				SensornetOperator op = (SensornetOperator) opIter.next();
				Iterator<SensornetOperator> childOpIter = op.childOperatorIterator();
				while (childOpIter.hasNext()) {
					SensornetOperator childOp = childOpIter.next();
					out.print("\"" + childOp.getID() + "\"" + edgeSymbol + "\""
							+ op.getID() + "\" ");				
					out.print("[fontsize=9 label = \" ");
					try {
						if (showTupleTypes) {
							out.print("type: " + 
								childOp.getLogicalOperator().getTupleAttributesStr(3) + " \\n");
						}
					} catch (TypeMappingException e1) {
						String msg = "Problem getting tuple attributes. " + e1;
						logger.warn(msg);
					}
					//print subclass attributes
//TODO: Not sure what this is for					
//					if (edgeLabelBuff.get(e.getID()) != null) {
//						out.print(edgeLabelBuff.get(e.getID()));
//					}
					out.print("\"];\n");
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
