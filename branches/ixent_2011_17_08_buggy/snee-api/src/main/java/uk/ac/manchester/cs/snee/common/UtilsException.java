package uk.ac.manchester.cs.snee.common;

public class UtilsException extends Exception {

	public UtilsException(String message) {
		super(message);
	}

	public UtilsException(String message, Exception e) {
		super(message, e);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
