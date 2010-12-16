/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.excep;

public class DATInvalidParamException extends DATException {

	private static final long serialVersionUID = -8495421382627299599L;

	public DATInvalidParamException(){
		super("The parameters for this DAT are invalid");
	}
}
