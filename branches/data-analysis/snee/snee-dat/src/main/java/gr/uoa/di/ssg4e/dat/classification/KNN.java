/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.classification;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.SNEEqlQuery;
import gr.uoa.di.ssg4e.query.excep.ParserException;

public class KNN extends AbstractClassifier {

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


	protected KNN(Classifiers method, String modelName){
		super(method, modelName);
	}

	protected KNN(Classifiers method, String modelName, String[] input,
			String output, String fromQry) throws ParserException, DATException {
		super(method, modelName, input, output, fromQry);
	}

	@Override
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
		 * an exception is thrown.
		 * 
		 * FIXME: This is not necessarily true. It could exist in the WHERE clause */
		if ( predAttrIdx < 0 )
			throw new DATException("The derived attribute <" + derivedAttributeName + "> " +
					"is not present in the SELECT clause of the DDLStatement");
	}

	@Override
	protected void validateQuery(SNEEqlQuery q, int datIndex)
			throws DATException {
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

	@Override
	protected void createClassifier() throws DATException {
		// TODO Auto-generated method stub
		
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

	@Override
	/**
	 * The refactoring should provide a valid query that can be run as a SNEEql
	 * query in SNEE. The query consists of two parts:
	 * 
	 * 1) Find the closest values to the input value that we wish to predict
	 * 2) Compute the predicted value
	 * 
	 * The first is handled using the getDistance, which computes the distance
	 * of two tuples on Euclidean space. Alternatively, one may use the
	 * getManhattanDistance() method to compute the distance based on the L1
	 * norm
	 * 
	 * The second step is by averaging the predicted attribute of the top-K values
	 * that step 1 returned
	 * 
	 * */
	public String refactorQuery(SNEEqlQuery query, int datIndex)
			throws DATException {

		/* First thing is to validate the query as a DAT query. If the query is valid,
		 * we get its bindings as a result */
		validateQuery(query, datIndex);

		/* 
		 * Depending on the last argument we will make some adjustments to the initial query
		 * 
		 * FIXME At this point, the query is returned intact */
		return query.toString();
	}


	/**
	 * This method returns the distance measure, using the bound variables.
	 * The distance is unnamed, so that it may be used both in the SELECT
	 * and the WHERE clause
	 * 
	 * @param predictTupleIter The name of the tuple iterator of the table
	 * whose values we want to predict
	 * 
	 * @param dataModelTupleIter The name of the tuple iterator containing
	 * the values that are used as the knowledge base (data model), based
	 * on which we predict the output value
	 * 
	 * */
	private String getDistance(String predictTupleIter, String dataModelTupleIter){

		StringBuilder strBld = new StringBuilder();

		strBld.append("SQRT( ");
		for ( int i = 0; i < bindings.length; i++ )
			strBld.append('(').
			append(predictTupleIter).append(".").append(input_Xi[i]).append(" - ").append(
					dataModelTupleIter).append(".").append(bindings[i]).append(") * (").
					append(predictTupleIter).append(".").append(input_Xi[i]).append(" - ").append(
							dataModelTupleIter).append(".").append(bindings[i]).append(") +");

		strBld.setLength(strBld.length() - 2);
		strBld.append(")");

		return strBld.toString();
	}

	/**
	 * This method returns the distance measure, using the bound variables,
	 * based on the Manhattan distance (L1 norm). The distance is unnamed, 
	 * so that it may be used both in the SELECT and the WHERE clause
	 * 
	 * @param predictTupleIter The name of the tuple iterator of the table
	 * whose values we want to predict
	 * 
	 * @param dataModelTupleIter The name of the tuple iterator containing
	 * the values that are used as the knowledge base (data model), based
	 * on which we predict the output value
	 * 
	 * */
	private String getManhattanDistance(String predictTupleIter, String dataModelTupleIter){

		StringBuilder strBld = new StringBuilder();

		strBld.append("SQRT( ");
		for ( int i = 0; i < bindings.length; i++ )
			strBld.append("abs(").
			append(predictTupleIter).append(".").append(input_Xi[i]).append(" - ").append(
					dataModelTupleIter).append(".").append(bindings[i]).append(") + ");

		strBld.setLength(strBld.length() - 2);
		strBld.append(")");

		return strBld.toString();
	}

}
