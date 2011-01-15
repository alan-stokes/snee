package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.GraphUtils;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * Utility class for displaying LAF.
 */
public class LAFUtils extends GraphUtils {
	
	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(LAFUtils.class.getName());
	
	/**
	 * LAF to be displayed.
	 */
	private LAF laf;
	
	/**
	 * Determines whether collection type (STREAM, WINDOW etc.) displayed.
	 */
	protected boolean showOperatorCollectionType = false;
	
	/**
	 * Determines whether tuple type (a1:t1,...,an:tn) displayed.
	 */
	protected boolean showTupleTypes = false;
	
	/**
	 * Determines whether node id is displayed.
	 */
	protected boolean showOperatorID = false;

	/**
	 * Name of item to be displayed.
	 */
	protected String name;
	
	/**
	 * Tree to be displayed.
	 */
	protected Tree tree;
	
	/**
	 * Constructor for LAFUtils.
	 * @param laf
	 */
	public LAFUtils(LAF laf) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER LAFUtils()"); 		
		this.laf = laf;
		this.name = laf.getID();
		this.tree = laf.getOperatorTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN LAFUtils()");
	}

	/**
	 * Generate a DOT file representation of the DLAF for Graphviz.
	 */
	protected void exportAsDOTFile(String fname) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER exportAsDOTFile()");
		exportAsDOTFile(fname, "", new TreeMap<String, StringBuffer>(), 
				new TreeMap<String, StringBuffer>(), new StringBuffer());
		if (logger.isTraceEnabled())
			logger.trace("RETURN exportAsDOTFile()");
		
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
				LogicalOperator op = (LogicalOperator) opIter.next();
				out.print("\"" + op.getID() + "\" [fontsize=9 ");

//				if (op instanceof ExchangeOperator) {
//					out.print("fontcolor = blue ");
//				}

				out.print("label = \"");
				if (showOperatorCollectionType) {
					out.print("(" + op.getOperatorDataType().toString()
							+ ") ");
				}
				out.print(op.getOperatorName() + "\\n");

				if (op.getParamStr() != null) {
					out.print(op.getParamStr() + "\\n");
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
				LogicalOperator op = (LogicalOperator) opIter.next();
				Iterator<LogicalOperator> childOpIter = op.childOperatorIterator();
				while (childOpIter.hasNext()) {
					LogicalOperator childOp = childOpIter.next();
					out.print("\"" + childOp.getID() + "\"" + edgeSymbol + "\""
							+ op.getID() + "\" ");				
					out.print("[fontsize=9 label = \" ");
					try {
						if (showTupleTypes) {
							out.print("type: " + 
								childOp.getTupleAttributesStr(3) + " \\n");
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
			logger.warn("Failed to write LAF to " + fname + ".", e);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN exportAsDOTFile()");
		}
	}
	
	/**
	 * Generates graph PNG image of given algebraic form.
	 */
	public void generateGraphImage() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER generateGraphImage()");
		try {
			String sep = System.getProperty("file.separator");
			String outputDir = SNEEProperties.getSetting(
					SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
					sep + laf.getQueryName() + sep + "query-plan";
			String dotFilePath = outputDir + sep + this.name + ".dot";
			exportAsDOTFile(dotFilePath);
			super.generateGraphImage(dotFilePath);
		} catch (Exception e) {
		    logger.warn("Problem generating LAF image: ", e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN generateGraphImage()");
	}	
}
