/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.sampling;


import gr.uoa.di.ssg4e.dat.DataAnalysisTechnique;
import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.excep.ParserException;


/**
 * 
 */
public abstract class AbstractSampler extends DataAnalysisTechnique {

	private Sampling _method = null;

	protected AbstractSampler(String modelName){
		super(modelName);
	}

	protected AbstractSampler(String modelName, String[] input, String output,
			String fromQry) throws ParserException, DATException {
		super(modelName, input, output, fromQry);
		// TODO Auto-generated constructor stub
	}

}
