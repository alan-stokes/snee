/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.classification;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.SNEEqlQuery;
import gr.uoa.di.ssg4e.query.excep.ParserException;

public class KNN extends AbstractClassifier {

	protected KNN(Classifiers method, String modelName){
		super(method, modelName);
	}

	protected KNN(Classifiers method, String modelName, String[] input,
			String output, String fromQry) throws ParserException, DATException {
		super(method, modelName, input, output, fromQry);
	}

	@Override
	public String refactorQuery(SNEEqlQuery q, int datIndex)
			throws DATException {
		return null;
	}

	@Override
	protected void validateParameters() throws DATException, ParserException {
	}

	@Override
	protected void validateQuery(SNEEqlQuery q, int datIndex)
			throws DATException {
	}

	@Override
	protected void createClassifier() throws DATException {
		// TODO Auto-generated method stub
		
	}
}
