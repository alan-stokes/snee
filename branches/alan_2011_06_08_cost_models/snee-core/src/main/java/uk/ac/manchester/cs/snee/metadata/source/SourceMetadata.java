package uk.ac.manchester.cs.snee.metadata.source;

import java.util.List;

public abstract class SourceMetadata extends SourceMetadataAbstract {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5648396445597689774L;

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