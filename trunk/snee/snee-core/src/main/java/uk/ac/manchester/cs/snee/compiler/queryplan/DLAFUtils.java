package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

/**
 * Utility class for displaying DLAF.
 */
public class DLAFUtils extends LAFUtils {

    /**
     * Logger for this class.
     */
    private static Logger logger = Logger.getLogger(DLAFUtils.class.getName());
	
	/**
	 * The DLAF to be displayed.
	 */
	private DLAF dlaf;
	
	/**
	 * DLAFUtils constructor
	 * @param dlaf
	 */
	public DLAFUtils(DLAF dlaf) {		
		super(dlaf.getLAF());
		if (logger.isDebugEnabled())
			logger.debug("ENTER DLAFUtils() " + dlaf);		
		this.dlaf = dlaf;
		this.name = dlaf.getID();
		this.tree = dlaf.getOperatorTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN DLAFUtils()");			
	}

	/**
	 * Generate a DOT file representation of the DLAF for Graphviz.
	 */
	protected void exportAsDOTFile(String fname,
			String label,
			TreeMap<String, StringBuffer> opLabelBuff,
			TreeMap<String, StringBuffer> edgeLabelBuff,
			StringBuffer fragmentsBuff) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER exportAsDOTFile() " + fname + ", " +
					label);
		StringBuffer buffer = new StringBuffer();
		buffer.append("\\nSources:");
		for (SourceMetadataAbstract source : dlaf.getSources()) {
			buffer.append("\\n\\t");
			buffer.append(source.getSourceName());
			buffer.append("(");
			buffer.append(source.getSourceType().name().toLowerCase());
			buffer.append(")");
		}
		String str = buffer.toString();
		super.exportAsDOTFile(fname, str, new TreeMap<String, StringBuffer>(), 
				new TreeMap<String, StringBuffer>(), new StringBuffer());
		if (logger.isTraceEnabled())
			logger.trace("RETURN exportAsDOTFile()");
	}
	
}
