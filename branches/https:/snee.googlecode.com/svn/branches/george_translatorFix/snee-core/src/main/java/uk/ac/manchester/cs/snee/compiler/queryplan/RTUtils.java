package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class RTUtils extends PAFUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(RTUtils.class.getName());
	
	/**
	 * Routing tree to be displayed.
	 */
	private RT rt;

	/**
	 * Determines whether site properties are displayed.
	 */
	private boolean displaySiteProperties = false;

//	/**
//	 * Determines whether link properties are displayed.
//	 */
//	private boolean displayLinkProperties = false;
	
	/**
	 * Constructor for RTUtils.
	 */
	public RTUtils(RT rt) {
		super(rt.getPAF());
		if (logger.isDebugEnabled())
			logger.debug("ENTER RTUtils()");
		this.rt = rt;
		this.name = rt.getID();
		this.tree = rt.getSiteTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN RTUtils()");
	}

	 /** {@inheritDoc} */
	protected void exportAsDOTFile(String fname,
			String label,
			TreeMap<String, StringBuffer> opLabelBuff,
			TreeMap<String, StringBuffer> edgeLabelBuff,
			StringBuffer fragmentsBuff) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER RTUtils.exportAsDOTFile() with file:" + 
					fname + "\tlabel: " + label);
		}		
		try {
		    final PrintWriter out = new PrintWriter(new BufferedWriter(
			    new FileWriter(fname)));
		    String edgeSymbol;
			out.println("digraph \"" + this.rt.getID() + "\" {");
			edgeSymbol = "->";
		    out.println("size = \"8.5,11\";"); // do not exceed size A4
		    out.println("label = \"" + label + "\";");
		    out.println("rankdir=\"BT\";");
		    Iterator<Site> siteIter = 
		    	this.tree.nodeIterator(TraversalOrder.POST_ORDER);
		    while (siteIter.hasNext()) {
				final Site currentSite = siteIter.next();
				out.print(currentSite.getID() + " [fontsize=20 ");
				if (currentSite.isSource()) {
					out.print("shape = doublecircle ");
				}
				out.print("label = \"" + currentSite.getID());
				if (this.displaySiteProperties) {
					out.print("\\nenergy stock: " + currentSite.getEnergyStock() + "\\n");
					out.print("RAM: " + currentSite.getRAM() + "\\n");
//					out.print(currentSite.getFragmentsString() + "\\n");
//					out.print(currentSite.getExchangeComponentsString());
				}
				out.println("\"];");
		    }
		    //traverse the edges now
		    siteIter = this.tree.nodeIterator(TraversalOrder.POST_ORDER);
		    while (siteIter.hasNext()) {
				final Site currentSite = siteIter.next();
				if (currentSite.getOutputs().length==0)
					continue;
				Site parentSite = (Site) currentSite.getOutput(0);
				
				out.print("\"" + currentSite.getID() + "\""
						+ edgeSymbol + "\""
						+ parentSite.getID() + "\" ");

//				if (this.displayLinkProperties) {
//					//TODO: find a more elegant way of rounding a double to 2 decimal places
//					out.print("[label = \"radio loss =" 
//					+ (Math.round(e.getRadioLossCost() * 100))
//					/ 100 + "\\n ");
//				out.print("energy = " + e.getEnergyCost() + "\\n");
//				out.print("latency = " + e.getLatencyCost() + "\"");
//				} else {
				    out.print("[");
//				}

				out.println("style = dashed]; ");
			}		    out.println("}");
		    out.close();

		} catch (Exception e) {
			logger.warn("Failed to write RT to " + fname + ".", e);		
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN RTUtils.exportAsDOTFile()");
		}
	}
	
}
