package uk.ac.manchester.cs.snee.metadata.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SourceMetadata extends SourceMetadataAbstract {

	public SourceMetadata(String sourceName, List<String> extentNames,
			SourceType sourceType) {
		super(sourceName, extentNames, sourceType);
	}
	
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
//		for (String extent : _extentRates.keySet()) {
//			s.append("\tExtent " + extent + " rate: " + 
//					_extentRates.get(extent));
//		}
		return s.toString();
	}

}