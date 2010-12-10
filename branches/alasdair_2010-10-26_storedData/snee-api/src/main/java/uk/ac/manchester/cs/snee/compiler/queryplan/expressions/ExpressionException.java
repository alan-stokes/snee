package uk.ac.manchester.cs.snee.compiler.queryplan.expressions;

/**
 * An exception raised if a problem arises when parsing the network
 * topology file associated with a sensor network data source.
 *
 */
public class ExpressionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1026710816037022011L;

	public ExpressionException(String message) {
		super(message);
	}

	public ExpressionException(String msg, Throwable e) {
		super(msg, e);
	}
	
}
