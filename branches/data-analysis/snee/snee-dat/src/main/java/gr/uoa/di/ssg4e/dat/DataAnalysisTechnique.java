/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat;

import gr.uoa.di.ssg4e.dat.classification.AbstractClassifier;
import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.dat.outlierdetection.AbstractOutlierDetection;
import gr.uoa.di.ssg4e.dat.schema.DATMetadata;
import gr.uoa.di.ssg4e.query.SNEEqlQuery;
import gr.uoa.di.ssg4e.query.excep.ParserException;

public abstract class DataAnalysisTechnique  {

	/************************************************************************************************
	 *	 					PATHS AND METADATA FILENAMES THAT ARE REQUIRED BY A DAT					* 
	 ************************************************************************************************/

	/** This is the path where the basic information about any DAT can be found. This path
	 * is shared among all DATs, in order to store metadata */
	protected static final String DATsPath = "./";

	/** Path where all data analysis models metadata are stored */
	private static final String DATMetadataFilename = "dat.xml";


	/************************************************************************************************
	 * 					VARIABLES THAT ARE COMMONLY SHARED BY ALL DATs REGARDLESS					* 
	 ************************************************************************************************/

	/** This is the name of the created DAT, as specified in the CREATE statement */
	protected final String modelName;

	/** Array with the input parameters, as specified in the CREATE statement */
	protected String[] parameters;

	/** The derived attribute, as specified in the CREATE statement */
	protected String derivedAttributeName;

	/** The query found in the FROM clause, as it was given in the CREATE statement.
	 * TODO: Perhaps I do not need a SNEEqlQuery but a simple String will do */
	protected String datSource;
//	protected SNEEqlQuery datSource;


	
	/************************************************************************************************
	 *	 					METHODS THAT ARE COMMON IN ORDER TO MANIPULATE A DAT 					* 
	 ************************************************************************************************/

	/** 
	 * This is a private constructor of the Data Analysis Technique class,
	 * used in order to create the necessary 
	 *  */
	static {
//		File f = new File(DATsPath + DATMetadataFilename);
//
//		try { /* Try and create the file */
//			if ( !f.exists() )
//				f.createNewFile();
//		} catch (final IOException e) {
//			e.printStackTrace();
//		}
	}

	protected DataAnalysisTechnique(String modelName){
		this.modelName = modelName;
	}

	/** Base Constructor of any DAT. The DAT must have a modelName */
	protected DataAnalysisTechnique( String modelName, String[] datParams, String output,
			String sourceQuery ) throws ParserException, DATException {

		this.modelName = modelName;
		parameters = datParams;
		derivedAttributeName = output;
		datSource = sourceQuery;

		/* Since all of the input is now known, we need to validate the parameters that
		 * have been passed to the classifier. The validation is implementation-specific */
		validateParameters();
	}

	/** 
	 * This method checks if the parameters specified when constructing an instance of
	 * a classifier are valid 
	 * @throws DATException in case the parameters of the DAT are incorrect
	 * */
	protected abstract void validateParameters() throws DATException, ParserException;


	/**
	 * Abstract method used to refactor a query that we already know that it contains a DAT.
	 * The query has already been parsed and we know which are the arguments for each of the
	 * clauses. We also know which source is the Data Analysis Technique (DAT).
	 * 
	 * @param query: The query that was found to contain a DAT source
	 * @param datIndex: The index of the source that is the DAT
	 * 
	 * @throws DATException: in case the datIndex does not refer to a correct index or that
	 * source is not a DAT 
	 * */
	public abstract String refactorQuery(SNEEqlQuery q, int datIndex) throws DATException;


	/**
	 * This method is used to validate a given query
	 * */
	protected abstract void validateQuery( SNEEqlQuery q, int datIndex ) throws DATException;

	/** This method is used to load the classifier with modelName from a file */
	public static DataAnalysisTechnique loadDAT( String modelName, DATMetadata metadata ) 
	throws DATException, ParserException{

		String[] params = null;
		if ( metadata.getParameters() != null )
			params = metadata.getParameters().toArray(new String[0]);
		String output = metadata.getDerivedAttribute();
		String sourceQuery = metadata.getSourceQuery();

		switch ( metadata.getDatType().getType() ){
		case ASSOCIATION_RULE:
			break;
		case CLASSIFIER:
			return AbstractClassifier.loadDAT(modelName, metadata.getDatType(), params, 
					output, sourceQuery);
		case CLUSTER:
			break;
		case OUTLIER_DETECTION:
			return AbstractOutlierDetection.loadDAT(modelName, metadata.getDatType(),
					params, output, sourceQuery);
		case PROBABILITY_FUNCTION:
			break;
		case SAMPLING:
			break;
		default:
			throw new DATException("Model name " + modelName + " does not refer to a DAT");
		}

		return null;
	}

	protected static DataAnalysisTechnique loadDAT( String modelName, DATSubType subType,
			String[] datParameters, String output, String sourceQuery ) 
	throws DATException, ParserException{
		throw new DATException("Unspecified parameters to load a Data Analysis Technique");
	}
}
