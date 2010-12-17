/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

/**
 * 
 */
public class IException extends Exception {
	private static final long serialVersionUID = -5001774393582590464L;

	/**
	 * Generic lookup exception extended by specific
	 * lookup exceptions.  
	 * @param message
	 */
	public IException(String message) {
		super(message);
	}

	public IException(String msg, Throwable e) {
		super(msg, e);
	}
}
