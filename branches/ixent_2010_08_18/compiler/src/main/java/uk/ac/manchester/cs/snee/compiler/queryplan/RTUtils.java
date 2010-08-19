package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;

public class RTUtils extends PAFUtils {

	private RT rt;
	
	public RTUtils(RT rt) {
		super(rt.getPAF());
		this.rt = rt;
		this.name = rt.getName();
		this.tree = rt.getSiteTree();
	}

//	protected void exportAsDOTFile(String fname) 
//	throws SchemaMetadataException {
//		String source = this.dlaf.getSource().getSourceName();
//		String sourceType = this.dlaf.getSourceType().name().toLowerCase();
//		String str = "\\nSource = "+source+" ("+sourceType+")";
//		exportAsDOTFile(fname, str, new TreeMap<String, StringBuffer>(), 
//				new TreeMap<String, StringBuffer>(), new StringBuffer());
//	}
	
}
