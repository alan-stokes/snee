package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;

public class PAFUtils extends DLAFUtils {

	private PAF paf;
	
	public PAFUtils(PAF paf) {
		super(paf.getDLAF());
		this.paf = paf;
		this.name = paf.getName();
		this.tree = paf.getOperatorTree();
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
