package uk.ac.manchester.cs.snee.common;

public class SNEEConfigurationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5698934085922328927L;

	public SNEEConfigurationException(String message) {
		super(message);
	}
	
	public SNEEConfigurationException(String message, Throwable e) {
		super(message);
	}
	
	public SNEEConfigurationException(Throwable e) {
		super(e);
	}

}
