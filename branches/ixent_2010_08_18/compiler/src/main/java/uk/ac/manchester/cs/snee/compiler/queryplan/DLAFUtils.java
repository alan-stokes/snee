package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;

public class DLAFUtils extends LAFUtils {

	private DLAF dlaf;
	
	public DLAFUtils(DLAF dlaf) {
		super(dlaf.getLAF());
		this.dlaf = dlaf;
		this.name = dlaf.getName();
		this.tree = dlaf.getOperatorTree();
	}

	protected void exportAsDOTFile(String fname) 
	throws SchemaMetadataException {
		String source = this.dlaf.getSource().getSourceName();
		String sourceType = this.dlaf.getSourceType().name().toLowerCase();
		String str = "\\nSource = "+source+" ("+sourceType+")";
		exportAsDOTFile(fname, str, new TreeMap<String, StringBuffer>(), 
				new TreeMap<String, StringBuffer>(), new StringBuffer());
	}
	
}
