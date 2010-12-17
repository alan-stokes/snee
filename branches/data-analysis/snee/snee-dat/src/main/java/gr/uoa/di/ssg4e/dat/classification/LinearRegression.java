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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

class LinearRegression extends AbstractClassifier {

	/**
	 * This array contains the attributes that are present in the SELECT clause,
	 * which are used to construct the LinearRegression classifier, i.e. compute
	 * the values (a,b) 
	 * */
	private String[] input_Xi;

	/** 
	 * This string contains the query that needs to be issued to compute (a,b)
	 * values
	 *  */
	private String abCompStr;

	private String[] bindings = null;

	private List<Integer> bindingIndexes = null;

	/** 
	 * This is a public constructor that creates a LinearRegression classifier
	 *  */
	public LinearRegression( String modelName ){
		super(Classifiers.LINEAR_REGRESSION, modelName);
	}

	/**
	 * This is the public constructor of a Linear Regression Classifier.
	 * 
	 * @throws DATException
	 * 
	 * */
	public LinearRegression(String modelName, String[] input, String outputParam, 
			String sourceQuery) throws ParserException, DATException {
		super(Classifiers.LINEAR_REGRESSION, modelName, input, outputParam, sourceQuery);
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

		/* Create a SNEEql query for the dat source */
		SNEEqlQuery q = new SNEEqlQuery(datSource);

		String[] selArgs = q.getSelectArgs();

		String tableIter = "";
		String derAttr = derivedAttributeName;
		if ( q.getTupleIterators() != null && (!q.getTupleIterators()[0].isEmpty()) ){
			tableIter = q.getTupleIterators()[0] + ".";
			derAttr = tableIter + derivedAttributeName;
		}

		/* One of the arguments is the output parameter. Skip it */
		input_Xi = new String[selArgs.length - 1];
		for ( int i = 0; i < selArgs.length; i++ ){
			if ( selArgs[i].equals(derAttr) )
				predAttrIdx = i; /* store the index of the predicted attribute */
			else
				input_Xi[i - (predAttrIdx < 0 ? 0 : 1 )] = selArgs[i].replace(tableIter, "");
		}

		/* In case the derived attribute does not exist in the SELECT clause, then
		 * an exception is thrown */
		if ( predAttrIdx < 0 )
			throw new DATException("The derived attribute <" + derivedAttributeName + "> " +
					"is not present in the SELECT clause of the DDLStatement");
	}

	/** This method returns the string that can be used to compute the values of (a,b)
	 * for the linear regression technique. It takes inp_x and inp_y as parameters,
	 * where <b>inp_x</b> is the X parameter used as input and <b>inp_y</b> is the 
	 * Y parameter, aka the derived attribute
	 * 
	 *  TODO: There may be a problem with the names 'sum_x', 'sum_y', 'sum_xy', 'sum_sqr_x',
	 *  if these are also used in <i>source</i>.
	 *  */
	protected void createClassifier(){

		/* The derived attribute exists. Replace in the template query inp_x and inp_y */
		if ( abCompStr == null )
			abCompStr = getABComputationString(input_Xi[0], derivedAttributeName);
	}

	private String getABComputationString(String inp_x, String inp_y){

		return 
			"SELECT	( S.n*S.sum_xy - S.sum_y * S.sum_x ) / ( S.n * S.sum_sqr_x - S.sum_x * S.sumx_x ) as a," +
			"		( S.sum_y * S.sum_sqr_x - S.sum_x * S.sum_xy ) / ( S.n * S.sum_sqr_x - S.sum_x * S.sum_x ) as b " +
			"FROM (" +
			"	SELECT	COUNT(*) as n," +
			"			SUM( S." + inp_x + " * S." + inp_y + " ) as sum_xy," +
			"			SUM( S." + inp_x + " ) as sum_x," +
			"			SUM( S." + inp_y + " ) as sum_y," +
			"			SUM( S." + inp_x + " * S." + inp_x + " ) as sum_sqr_x" +
			"	FROM ( " + super.datSource.toString() + " ) S " +
			") S ";
	}

	/** 
	 * This method is used to refactor a query that contains a DAT (LinearRegression source
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
		String datItr = query.getTupleIterators()[datIndex];
		String derAtt = datItr + "." + this.derivedAttributeName; 
		String[] args = query.getSelectArgs();

		/* Create the SELECT-clause */
		StringBuilder sb = new StringBuilder(query.getPrefix());
		sb.append(SNEEqlQuery.selectStr);
		for ( int i = 0; i < args.length; i++ )
			sb.append(' ').append(args[i].replaceAll("\\b" + derAtt + "\\b",
					datItr + ".a * " + bindings[0].replaceAll("[() ]", "") + 
					" + " + datItr + ".b") ).append(",");

		sb.setCharAt(sb.length() - 1, ' ');
		sb.append(SNEEqlQuery.fromStr);

		/* Create the FROM-clause */
		args = query.getFromArgs();
		for ( int i = 0; i < args.length; i++ ){

			sb.append(' ');

			if ( i != datIndex )
				sb.append(args[i]);
			else
				sb.append('(').append(abCompStr).append(')');

			sb.append(' ').append(query.getTupleIterators()[i]).append(',');
		}

		sb.deleteCharAt(sb.length() - 1);

		/* If there were more conditions in the WHERE clause other than the simple
		 * binding of values, then we need to add a WHERE clause. The WHERE clause
		 * definitely exists, otherwise an exception would have been thrown already */
		args = query.getWhereArgs();
		if ( args.length - input_Xi.length != 0 ){
			sb.append(' ').append(SNEEqlQuery.whereStr);

			int j = 0;
			for ( int i = 0; i < args.length - 1; i++ ){

				if ( i == bindingIndexes.get(j) ){ /* The i-th value was bound */

					//TODO: sb.append( args[i].replaceAll("\\w+", "") );
					sb.append( args[i].replaceAll("\\w+", "1=1") );
					bindingIndexes.remove(0);
					++j;
				}else
					sb.append(args[i]);

				if ( i != args.length - 1 )
					sb.append(query.getWhereConditions()[i]);
			}
		}

		sb.append(query.getSuffix());

		for (int i = 0; i < bindings.length; i++ )
			bindings[i] = null;
		bindings = null;

		return sb.toString();
	
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

		/* 2) The derived attribute must exist in the SELECT or in the WHERE clause */
		int i = 0;
		for ( ; i < q.getSelectArgs().length; i++ )
			if ( q.getSelectArgs()[i].contains(derAtt) )
				break;

		if ( i == q.getSelectArgs().length ){
			for ( i = 0; i < q.getWhereArgs().length; i++ )
				if ( q.getWhereArgs()[i].contains(derAtt) )
					break;

			if ( i == q.getWhereArgs().length )
				throw new DATException(
						"The derived attribute does not exist in the SELECT or the WHERE clause " +
						"of the query");
		}

		/* 3) All input variables must have a binding value. FIXME: Is this correct? */
		bindings = getBindings(q.getWhereArgs(), q.getWhereConditions(),
				q.getTupleIterators()[datIndex]);
		if ( bindings == null )
			throw new DATException("Not all input variables are bound!");
	}

	/**
	 * This method discovers the bindings of the values for each of the
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
		for ( int i = 0; i < input_Xi.length; i++ ){

			int j = 0;
			for ( ; j < whereArgs.length; j++ ){

				/* A binding means that there is an equality, and we are
				 * only interested in a single equality. In case of more
				 * equalities we return a null value */
				String[] eqSides = whereArgs[j].split("=");
				if ( eqSides.length == 2 ){

					/* If one of the two parts of the equality is the input value, the other
					 * part is the bound value */
					if ( eqSides[0].matches("\\b" + datItrName + "\\." + input_Xi[i] + "\\b") )
						bindings[i] = eqSides[1];
					else if ( eqSides[1].matches("\\b" + datItrName + "\\." + input_Xi[i] + "\\b") )
						bindings[i] = eqSides[0];

					/* If the DAT property was bound, some additional checks must be performed */
					if ( bindings[i] != null ){

						/* Bindings can only be before / after a conjunction (AND). If not
						 * then we assume it is an error */
						if ( j != 0 && !whereConds[j - 1].equals("and") ){
							bindings[i] = null;
							break; 
						}

						if ( j != (whereArgs.length - 1) && !whereConds[j + 1].equals("and") ){
							bindings[i] = null;
							break; 
						}

						if ( !bindings[i].matches("[ ]*\\b.+\\..+\\b[ ]*") ){
							bindings[i] = null;
							break; 
						}

						/* The DAT field has been bound successfully. Add the condition index
						 * to the list of indexes that are bound */
						bindingIndexes.add(j);
						break;
					}

				}else if ( eqSides.length > 2 ){
					bindings[i] = null;
					break; 
				}else{
					//nonBindings;
				}
			}

			/* No binding was found for the i-th input. Can not proceed */
			if ( bindings[i] == null ){
				bindingIndexes.clear();
				bindingIndexes = null;
				bindings = null;
				break;
			}
		}

		/* Sort the values in the bound fields and return the bindings */
		if ( bindingIndexes != null )
			Collections.sort( bindingIndexes );
		return bindings;
	}
}