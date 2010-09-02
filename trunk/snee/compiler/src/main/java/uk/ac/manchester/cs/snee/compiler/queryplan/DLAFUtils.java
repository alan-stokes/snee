package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;

public class DLAFUtils extends LAFUtils {

	private DLAF dlaf;
	
	public DLAFUtils(DLAF dlaf) {
		super(dlaf.getLAF());
		this.dlaf = dlaf;
		this.name = dlaf.getName();
	}

	protected void exportAsDOTFile(String fname) 
	throws SchemaMetadataException {
		StringBuffer buffer = new StringBuffer();
		buffer.append("\\nSources:");
		for (SourceMetadata source : dlaf.getSources()) {
			buffer.append("\\n\\t");
			buffer.append(source.getSourceName());
			buffer.append("(");
			buffer.append(source.getSourceType().name().toLowerCase());
			buffer.append(")");
		}
		String str = buffer.toString();
		exportAsDOTFile(fname, str, new TreeMap<String, StringBuffer>(), 
				new TreeMap<String, StringBuffer>(), new StringBuffer());
	}
	
}
