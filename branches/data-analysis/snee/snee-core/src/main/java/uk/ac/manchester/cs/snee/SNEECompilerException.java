package uk.ac.manchester.cs.snee;


public class SNEECompilerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6993852086223848774L;


	public SNEECompilerException(String message) {
		super(message);
	}

	public SNEECompilerException(Throwable e) {
		super(e);
	}
	
	public SNEECompilerException(String msg, Throwable e) {
		super(msg, e);
	}

}
