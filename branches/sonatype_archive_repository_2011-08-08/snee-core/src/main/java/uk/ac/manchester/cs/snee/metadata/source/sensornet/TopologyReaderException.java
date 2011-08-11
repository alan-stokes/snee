package uk.ac.manchester.cs.snee.metadata.source.sensornet;

/**
 * An exception raised if a problem arises when parsing the network
 * topology file associated with a sensor network data source.
 *
 */
public class TopologyReaderException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1026710816037022011L;

	public TopologyReaderException(String message) {
		super(message);
	}

	public TopologyReaderException(String msg, Throwable e) {
		super(msg, e);
	}
	
}
