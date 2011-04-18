/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.outlierdetection;

import gr.uoa.di.ssg4e.dat.DATSubType;
import gr.uoa.di.ssg4e.dat.DataAnalysisTechnique;
import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.excep.ParserException;

public abstract class AbstractOutlierDetection extends DataAnalysisTechnique {

	/************************************************************************************************
	 * 					VARIABLES REQUIRED BY THE CLASSIFIER TO WORK PROPERLY						* 
	 ************************************************************************************************/

	/** This is the classification technique used, as specified in the CREATE statement */
	private final OutlierDetectors _method;

	/**
	 * Constructor that creates a new AbstractClassifier given the specific method
	 * and modelName. This constructor assumes that the information regarding the
	 * classifier are stored in an XML file which we are able to read from
	 * */
	protected AbstractOutlierDetection(OutlierDetectors method, String modelName){
		super(modelName);
		_method = method;
	}

	protected AbstractOutlierDetection(OutlierDetectors method, String modelName,
			String[] input, String output, String fromQry)
	throws ParserException, DATException{
		
		super( modelName, input, output, fromQry );
		_method = method;
	}

	/** This method is used to create a specific instance of a classifier, according
	 * to the name that has been provided. The output value is the derived attribute
	 * of the classifier, whereas the input strings contain the variables that will 
	 * be produced. The modelName is the name the user has requested to identify the
	 * classifier
	 * 
	 * <p>
	 * 
	 * Assume the following example:
	 * 		CREATE CLASSIFIER [linearRegressionFunction, pressure] forestLRF
	 * 		FROM fromList
	 * 
	 * Then <b>classifierName</b> = "linearRegressionFunction", <b>modelName</b> = "forstLRF", 
	 * <b>output</b> = "pressure", <b>input</b> = null. <b>fromList</b> contains the entire
	 * string found in the FROM clause of the above query.
	 * 
	 * @param classifierName: The name that identifies which classifier to use
	 * @param modelName: The name the user requests to refer to the created classifier
	 * @param output: The derived attribute of the classification process
	 * @param input: An array of strings with the input parameters of the classifer, as specified
	 * after the derived attribute.
	 * @param fromList: The string found in the FROM-clause of the CREATE query. Typically, this
	 * will be a SNEEql query. TODO: If there already exists a SNEEqlQuery type, use that one
	 * instead. This should greatly simplify matters
	 * @throws ParserException 
	 * @throws DATClassifierException 
	 *  */
	public static AbstractOutlierDetection loadDAT( String modelName, DATSubType subType,
			String[] datParameters, String output, String sourceQuery )
	throws DATException, ParserException{

		AbstractOutlierDetection od = null;

		switch ( subType ) {
		case D3:
			od = new D3OutlierDetection( modelName, datParameters, output, sourceQuery);
			break;

		default:
			break;
		}

		return od; 
	}
}
