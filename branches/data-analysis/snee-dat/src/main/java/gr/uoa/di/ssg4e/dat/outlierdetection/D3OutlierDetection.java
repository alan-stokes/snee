/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.outlierdetection;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.SNEEqlQuery;
import gr.uoa.di.ssg4e.query.excep.ParserException;

/**
 * Class used to implement the D3 Outlier Detection 
 * */
class D3OutlierDetection extends AbstractOutlierDetection {


	/**
	 * This array contains the attributes that are present in the SELECT clause,
	 * which are used to construct the LinearRegression classifier, i.e. compute
	 * the values (a,b) 
	 * */
	private String[] input_Xi = null;

	private String[] bindings = null;

	private List<Integer> bindingIndexes = null;

	int range = -1;
	double probability = -1.0;

	/** 
	 * This is a public constructor that creates a LinearRegression classifier,
	 * reading its parameters / input / options from a file
	 *  */
	public D3OutlierDetection( String modelName ){

		super(OutlierDetectors.D3, modelName);

		/* For simplicity, we assume that a LinearRegression classifier has already been
		 * constructed, given the query below
		 * 
		 * CREATE CLASSIFIER [linearRegressionFunction, pressure] forestLRF 
		 * FROM ( SELECT temperature, pressure
		 * 		  FROM forest [ NOW - 20 MIN TO NOW ] ); 
		 * 
		 * LinearRegression does not take any input parameters
		 * All of the information below could be stored in the XML file
		 *  */
		String[] attributes = new String[]{"pressure", "temperature"};
		parameters = null;
		derivedAttributeName = "pressure";
		input_Xi = new String[]{"pressure","temperature"};
		datSource = "SELECT temperature, pressure FROM forest [ NOW - 20 MIN TO NOW ]";

		String srcQuery = "SELECT s.temperature FROM someSrc s";
		String joinQuery = "flood[NOW]";
		range = 5;
	}

	/**
	 * This is the public constructor of a Linear Regression Classifier.
	 * 
	 * @throws DATException
	 * 
	 * */
	public D3OutlierDetection(String modelName, String[] input, String outputParam, 
			String sourceQuery) throws ParserException, DATException {

		super(OutlierDetectors.D3, modelName, input, outputParam, sourceQuery);
	}

	/** This method validates all of the parameters that have been specified in the
	 * 
	 * The predicted attribute must be one of the
	 * @throws DATClassifierException in case the parameters are not valid for a linear regression
	 * classifier 
	 * @throws SNEEqlException 
	 *  */
	protected void validateParameters() throws DATException, ParserException {

		int predAttrIdx = -1; /* This is the predicted attribute index */

		/* Create on the fly a SNEEql query. */
		SNEEqlQuery q = new SNEEqlQuery(datSource);

		String[] selArgs = q.getSelectArgs();
		q = null;

		/* One of the arguments is the output parameter */
		input_Xi = new String[selArgs.length - 1];

		for ( int i = 0; i < selArgs.length; i++ )
			if ( selArgs[i].equals(derivedAttributeName) )
				predAttrIdx = i; /* store the index of the predicted attribute */
			else
				input_Xi[i - (predAttrIdx < 0 ? 0 : 1 )] = selArgs[i];

		/* In case the derived attribute does not exist in the SELECT clause, then
		 * an exception is thrown */
		if ( predAttrIdx < 0 )
			throw new DATException("The derived attribute <" + derivedAttributeName + "> " +
					"is not present in the SELECT clause of the DDLStatement");
	}

	public String getD3Query(String joinTable, String joinTableItr){

		/* Depending on the last argument we will make some adjustments to the initial query */
		return getQ6(joinTable, joinTableItr, range);
	}

	/**
	 * 
	 * 
	 * */
	private String getQ1(){
		StringBuilder sb = new StringBuilder("SELECT RSTREAM ");
		for ( int i = 0; i < input_Xi.length; i++ ){
			sb.append("SUM(").append(input_Xi[i]).append(") as sum").append((i + 1)).
			append(",AVG(").append(input_Xi[i]).append(") as avg").append((i + 1)).append(',');
		}
		sb.append("COUNT(*) as cnt FROM (").append( datSource ).append(")");
		return sb.toString();
	}

	/** 
	 * Gets the number of attributes that have been requested and query q1
	 * 
	 * The form of the query we want to return is the following:
	 * 
	 * SELECT RSTREAM SQUAREROOT((SUM(t - avg))^2 / cnt) as sigma, cnt
	 * FROM flood[FROM NOW - 20 MIN TO NOW] t, Q1
	 *  */
	private String getQ2(){

		StringBuilder sb = new StringBuilder("SELECT RSTREAM ");
		for ( int i = 0; i < input_Xi.length; i++ )
			sb.append("SQRT(((SUM(t.").append(input_Xi[i]).append("-q1.avg").append(i+1).append(
					"))^2) / q1.cnt) as sigma").append(i+1).append(',');

		sb.append("q1.cnt as cnt FROM (").append( datSource ).append(") t, (").append(
				getQ1()).append(") q1");
		return sb.toString();
	}

	/**
	 * Creates a query that takes the size of the random sampling approach
	 * */
	private String getQ3(){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT RSTREAM COUNT(*) as rsize FROM ").append(modelName).append("_sample");
		return sb.toString();
	}

	/**
	 * Query 4 computes the bandwidth of the distribution, based on Scott's rule
	 * We also push the cnt from q2 towards the higher levels
	 * */
	private String getQ4(){

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT RSTREAM ");
		for ( int i = 0; i < input_Xi.length; i++ )
			sb.append("SQRT(5)*q2.sigma").append(i+1).append("*(q3.rsize^(-1/(4+").append(
					input_Xi.length).append("))) as scotts").append(i+1).append(',');

		sb.append("q2.cnt as cnt FROM (").append( getQ2() ).append(
				") q2, (").append( getQ3() ).append(") q3");

		return sb.toString();
	}

	private String getQ5(String joinTable, String joinTableItr, int range){

		int tItrSz = joinTableItr.length() + 1;

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT RSTREAM ");

		for ( int i = 0; i < input_Xi.length; i++ )
			sb.append("2*").append(range).append("-(1/(3*(q4.scotts").append(i+1).append("^2)))*((x.").append(
					bindings[i].substring(tItrSz)).append("-y.").append(input_Xi[i]).append("+").append(range).append(
							")^3 - (x.").append(bindings[i].substring(tItrSz)).append("-y.").append(input_Xi[i]).append(
									"-").append(range).append(")^3)+");

		sb.setCharAt(sb.length() - 1, ' ');
		sb.append(" as q, q4.cnt as cnt FROM (").append( getQ4() ).append(") q4,").append( 
				joinTable ).append(" x,").append(modelName).append("_sample y WHERE ");

		for ( int i = 0; i < input_Xi.length; i++ )
			sb.append("abs((x.").append(bindings[i].substring(tItrSz)).append("-y.").append(input_Xi[i]).append(
					")/q4.scotts").append(i+1).append(") < 1 AND ");
		sb.setLength(sb.length() - 4);

		return sb.toString();
	}

	private String getQ6(String joinTable, String joinTableItr, int range){

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT RSTREAM (1/q5.cnt)*((3/4)^").append(input_Xi.length).append(")*(1/(");
		for ( int i = 0; i < input_Xi.length; i++ )
			sb.append("q5.scotts").append(i+1).append("*");
		sb.setCharAt(sb.length() - 1, ')');
		sb.append("*q5.q as prob FROM (").append( getQ5(joinTable, joinTableItr, range) ).append(") q5");

		return sb.toString();
	}

	/** 
	 * This method is used to refactor a query that contains a DAT (D3OutlierDetection source
	 * in this case). The initial query has already been parsed and we know what are the
	 * specific arguments for each clause.
	 * 
	 *  @param datIndex: The index of the fromArgs that is the linear regression
	 *  
	 *  @throws DATException if the given query is not valid in terms of Data Analysis
	 *  */
	public String refactorQuery( SNEEqlQuery query, int datIndex ) throws DATException {

		/* First thing is to validate the query as a DAT query. If the query is valid,
		 * we get its bindings as a result */
		validateQuery(query, datIndex);

		/* Check if the derived attribute is in the SELECT clause of the query */
//		String datItr = query.getTupleIterators()[datIndex];
//		String derAtt = datItr + "." + this.derivedAttributeName; 
//		String[] args = query.getSelectArgs();

		/* Depending on the last argument we will make some adjustments to the initial query */
		return getQ6(query.getFromArgs()[1 - datIndex], query.getTupleIterators()[1 - datIndex], range);
	}

	@Override
	/**
	 * This method is used to validate the given query as a DAT query which can be
	 * successfully refactored. The checks that are performed are the following:
	 * 
	 * 1) The datIndex must be within the bounds of the array
	 * 2) The source found in the datIndex position must be a DAT
	 * 3) At least 2 sources are required, since we must bind the values of the DAT
	 * 4) There must be a WHERE clause
	 * 5) The derived attribute name must be either in the SELECT or in the WHERE clause.
	 * 		If it is not in either place, there is no point in issuing the query
	 * 6) All input variables of the DAT (as they were stored in the XML) must be bound
	 * 7) The derived attribute can not be bound (FIXME: why not?)
	 * 8) Bindings can only be under a conjunction. They can not be part of an OR statement.
	 * 
	 * */
	protected void validateQuery(SNEEqlQuery q, int datIndex) throws DATException {

		if ( datIndex < 0 || datIndex >= q.getFromArgs().length )
			throw new DATException("Illegal index: Value " + datIndex + " is out of bounds.");


		/* Linear Regression requires at least 2 sources: one DAT, one classic */
		if ( q.getFromArgs().length < 2 )
			throw new DATException("Linear Regression requires at least two sources in the FROM clause");

		/* 1) There must be a WHERE clause, because there must be a binding of
		 * the values */
		if ( q.getWhereArgs() == null )
			throw new DATException("There is no WHERE clause in the statement");

		/* Check if the derived attribute is in the SELECT clause of the query */
		String derAtt = q.getTupleIterators()[datIndex].isEmpty() ?
				this.derivedAttributeName : 
					(q.getTupleIterators()[datIndex] + "." + this.derivedAttributeName); 

		/* 3) All input variables must have a binding value. FIXME: Is this correct? */
		bindings = getBindings(q.getWhereArgs(), q.getWhereConditions(),
				q.getTupleIterators()[datIndex]);
		if ( bindings == null )
			throw new DATException("Not all input variables are bound!");
	}

	/**
	 * This method discovers the bindings of the values for each of the variables maintained
	 * by this Data Analysis Technique object
	 * 
	 *  @return The bindings. If an error occurs, a null value is returned
	 * 
	 *  FIXME: Bindings MUST NOT be in an OR condition
	 *  FIXME: (Scepticism) Can two different input values be bound to the same field?
	 *  */
	private String[] getBindings(String[] whereArgs, String[] whereConds, String datItrName){

		bindingIndexes = new LinkedList<Integer>();
		bindings = new String[input_Xi.length];

		/* Given that we know the input variables, we want to find how these are bound
		 * to other sources */

		/* For all of the equalities */
		for ( int j = 0; j < whereArgs.length; j++ ){

			/* Split on the = sign */
			String[] eqSides = whereArgs[j].split("=");
			if ( eqSides.length == 2 ){

				int i = -1; /* store the index that matches */

				/* Check which of the two parts of the equality is equal to the datItrName.
				 * If none is equal, we skip it */
				if ( eqSides[0].startsWith(datItrName + ".") )
					i = 1;
				else if ( eqSides[1].startsWith(datItrName + ".") )
					i = 0;

				/* In case one of the two matches, check which binding it refers to from
				 * the input_Xi array */
				if ( i >= 0 ){

					int k = 0;
					for ( ; k < input_Xi.length; k++ ){
						if ( eqSides[1-i].equals( datItrName + "." + input_Xi[k] )){
							bindings[k] = eqSides[i];
							bindingIndexes.add(k);
							break;
						}
					}

					/* If the DAT property was bound but not matched, then that is an error
					 * and we return null */
					if ( k == input_Xi.length ){
						bindings = null;
						return null;
					}
				}

			}else if ( eqSides.length > 2 ){ /* More than two equalities where encountered */
				bindings[j] = null;
				break; 
			}else{
				//nonBindings;
			}
		}

		/* Not all input values were bound! There will be a problem in this occasion! */
		if ( bindingIndexes.size() != input_Xi.length ){
			bindings = null;
			bindingIndexes = null;
		}

		return bindings;
	}

}
