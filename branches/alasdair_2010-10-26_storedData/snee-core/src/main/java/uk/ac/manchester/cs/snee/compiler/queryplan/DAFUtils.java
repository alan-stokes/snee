package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * Utility class for displaying PAF.
 */
public class DAFUtils extends PAFUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(DAFUtils.class.getName());
	
	/**
	 * PAF to be displayed.
	 */
	private DAF daf;

	private boolean displayExchangeOperatorRouting = false;
	
	/**
	 * Constructor for LAFUtils.
	 * @param laf
	 */	
	public DAFUtils(DAF daf) {
		super(daf.getPAF());
		if (logger.isDebugEnabled())
			logger.debug("ENTER DAFUtils()"); 
		this.daf = daf;
		this.name = daf.getID();
		this.tree = daf.getOperatorTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN DAFUtils()"); 
	}
	
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
	    Iterator<SensornetOperator> opIter = 
	    	this.tree.nodeIterator(TraversalOrder.POST_ORDER);
	    while (opIter.hasNext()) {
			final SensornetOperator op = (SensornetOperator) opIter.next();
		
			StringBuffer strBuff = new StringBuffer();
			if (opLabelBuff.containsKey(op.getID())) {
				strBuff = opLabelBuff.get(op.getID());
			}
			
			if (op instanceof SensornetExchangeOperator) {
	
			    if (displayExchangeOperatorRouting) {
					strBuff.append(((SensornetExchangeOperator) op)
						.getDOTRoutingString());
				    }
			}
			opLabelBuff.put(op.getID(), strBuff);
	    }
	       /**
	     * Draw the fragments
	     */
	    final Iterator<Fragment> f = daf.fragmentIterator(TraversalOrder.POST_ORDER);
	    while (f.hasNext()) {
	    	final Fragment frag = f.next();
	    	fragmentsBuff.append("subgraph cluster_" + frag.getID() + " {\n");
	    	fragmentsBuff.append("style=\"rounded,dashed\"\n");
	    	fragmentsBuff.append("color=red;\n");

			opIter = frag.getOperators().iterator();
			while (opIter.hasNext()) {
			    final SensornetOperator op = opIter.next();
			    fragmentsBuff.append("\"" + op.getID() + "\" ;\n");
			}
	
			final StringBuffer sites = new StringBuffer();
			boolean first = true;
			final Iterator<Site> sitesIter = frag.getSites().iterator();
			while (sitesIter.hasNext()) {
			    if (!first) {
				sites.append(",");
			    }
			    sites.append(sitesIter.next().getID());
			    first = false;
			}
	
			fragmentsBuff.append("fontsize=9;\n");
			fragmentsBuff.append("fontcolor=red;\n");
			fragmentsBuff.append("labelloc=t;\n");
			fragmentsBuff.append("labeljust=r;\n");
			fragmentsBuff.append("label =\"Fragment " + frag.getID());
			
			if (!sites.toString().equals("")) {
				fragmentsBuff.append("\\n {" + sites + "}");
			}	
			fragmentsBuff.append("\";\n}\n");
			
			//draw recursive loop
			if (frag.isRecursive()) {
					SensornetExchangeOperator parentExchOp = frag.getParentExchangeOperator();
					if (parentExchOp!=null) {
					    final SensornetOperator op = (SensornetOperator) parentExchOp.getInput(0);
					    fragmentsBuff.append(frag.getParentExchangeOperator().getID()
							    + " -> " + op.getID()
							    + " [headport=s tailport=n];\n");
					}
				}
		    }    	
	    super.exportAsDOTFile(fname, label, opLabelBuff, edgeLabelBuff, 
    			fragmentsBuff);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN exportAsDOTFile()");
		}		
	}		
}
