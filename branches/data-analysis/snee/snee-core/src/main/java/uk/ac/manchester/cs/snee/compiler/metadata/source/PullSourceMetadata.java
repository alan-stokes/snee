package uk.ac.manchester.cs.snee.compiler.metadata.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class PullSourceMetadata extends SourceMetadata {

	/**
	 * Publication rate for each extent
	 */
	protected Map<String, Double> _extentRates = 
		new HashMap<String, Double>();

	public PullSourceMetadata(String sourceName, List<String> extentNames,
			SourceType sourceType) {
		super(sourceName, extentNames, sourceType);
	}

	public void setRate(String extentName, double rate) {
		if (logger.isTraceEnabled())
			logger.trace("Rate set to " + rate + " for extent " + extentName);
		_extentRates.put(extentName, rate);
	}

	public double getRate(String extentName) 
	throws SourceMetadataException {
		double rate;
		if (_extentRates.containsKey(extentName)) {
			rate = _extentRates.get(extentName);
		} else if (_extentNames.contains(extentName)) {
			rate = 1.0;
		} else {	
			String msg = "Unknown extent name " + extentName;
			logger.warn(msg);
			throw new SourceMetadataException(msg);
		}
		return rate;
	}
	
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
		for (String extent : _extentRates.keySet()) {
			s.append("\tExtent " + extent + " rate: " + 
					_extentRates.get(extent));
		}
		return s.toString();
	}

}