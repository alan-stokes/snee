/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

import gr.uoa.di.ssg4e.query.excep.ParserException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple SNEEql Query class that understands the SELECT-FROM-WHERE clauses and splits a given query
 * to those individual statements. Identifies the SELECT arguments as well as the FROM sources.
 * 
 * TODO: The SNEEqlQuery should not be visible outside the scope of DATs. Is this possible without
 * major modifications?
 * */
public class SNEEqlQuery {

	/****************************************************************************
	 *	 					VARIABLES USED TO REFACTOR A QUERY 					* 
	 ****************************************************************************/
	public static final String fromStr = "from";
	public static final String selectStr = "select";
	public static final String whereStr = "where";

	/** This is to identify the FIRST match when searching for a given keyword */
	private static final int FIRST_MATCH = 0;

	/** This is to identify the LAST match when searching for a given keyword */
	private static final int LAST_MATCH = 1;

	/** What */
	private String[] selectArgs = null;
	private String[] fromArgs = null;
	private String[] whereArgs = null;
	private String[] whereCons = null;

	private SNEEqlSource[] sources = null;


	/** Tuple iterators for each source, in the same order as they are given in the query */
	private String[] tupleIterNames = null;

	private String prefix = null;
	private String suffix = null;

	/** This method is used to create SNEEql queries from the given input query */
	public SNEEqlQuery(String query) throws ParserException {

		/* We parse the pre-processed query */
		parse( preprocess(query) );

	}

	/** This method takes the given query and finds prefix and suffix that do not belong to
	 * the query itself, i.e. identifies opening and closing parentheses, semi-colons etc. */
	private String preprocess(String query) throws ParserException {

		query = query.toLowerCase();
		int prefixIdx = 0, suffixIdx = query.length() - 1;

		/* Ignore trailing spaces at the end */
		while ( Character.isWhitespace( query.charAt(suffixIdx) ) )
			--suffixIdx;

		if ( query.charAt(suffixIdx) == ';' )
			--suffixIdx;

		/* Remove opening and closing parentheses. Remove the semicolon at the end */
		while ( prefixIdx < suffixIdx ){

			/* skip whitespaces at the beginning of the query */
			while ( Character.isWhitespace( query.charAt(prefixIdx) ) )
				++prefixIdx;

			/* skip whitespaces at the end of the query */
			while ( Character.isWhitespace( query.charAt(suffixIdx) ) )
				--suffixIdx;

			/* Now, find if there are matching parentheses
			 * at the beginning and end of the query */
			if ( query.charAt(prefixIdx) == '(' && query.charAt(suffixIdx) == ')' ){
				++prefixIdx;
				--suffixIdx;
				continue;
			}

			/* If an opening parentheses was found but not a closing one,
			 * throw an exception. Malformed query. This is not the case for
			 * a closing parentheses, since it could be for a condition in
			 * the WHERE clause, e.g. q8 in the Queries.java files */
			if ( query.charAt(prefixIdx) == '(' )
				throw new ParserException();

			/* Neither opening, nor closing parentheses. Break */
			++suffixIdx;
			break;
		}

		prefix = query.substring(0, prefixIdx);
		suffix = query.substring(suffixIdx, query.length());

		return query.substring(prefixIdx, suffixIdx);
	}

	/** 
	 * This method parses the given query and identifies its distinct parts,
	 * namely the SELECT, FROM and WHERE clauses, along with their arguments
	 * 
	 * TODO: Create a WHERE-clause tree that will contain the arguments where
	 * leafs are the low level conditions and internal nodes are the AND/OR/NOT
	 *  */
	private void parse( String query ) throws ParserException{

		/* The query needs to have at least a length of "SELECT" + "FROM" with an intermediate
		 * space. It also needs additional information, it can not be equal either */
		if ( query.length() <= selectStr.length() + fromStr.length() + 1 )
			throw new ParserException();

		int selectIdx = findExactString(selectStr, query, FIRST_MATCH);
		if ( selectIdx < 0 )
			throw new ParserException();

		int fromIdx = findExactString(fromStr, query, FIRST_MATCH);
		if ( (fromIdx < 0) || (fromIdx < selectIdx) )
			throw new ParserException();

		/* whereIdx serves as sort of a suffix for the query */
		int whereIdx = findExactString(whereStr, query, LAST_MATCH);

		/* Provided there is a WHERE clause, check whether it is top-level or not.
		 * We need to know whether there is a SUFFIX that should be taken into account
		 * while parsing this particular query */
		if ( whereIdx >= 0 ){

			/* Count the opening and closing parentheses between the FROM and WHERE indexes */
			int parSum = 0; /* counting parentheses */
			for ( int i = fromIdx + fromStr.length(); i < whereIdx; i++ )
				if ( query.charAt(i) == '(' )
					++parSum;
				else if ( query.charAt(i) == ')' )
					if ( --parSum < 0 )	/* encountered closing parentheses without a match: error */
						throw new ParserException();

			if ( parSum > 0 )
				whereIdx = query.length();

		}else
			whereIdx = query.length();

		selectArgs = query.substring(selectIdx + selectStr.length(), fromIdx).split(",");
		for ( int i = 0; i < selectArgs.length; i++ )
			selectArgs[i] = selectArgs[i].trim();

		/* Split the FROM clause to see if a DAT is used. The FROM clause is split
		 * on commas. Also find the names of the tuple iterators for each source */
		fromArgs = parseFromClause( query.substring(fromIdx + fromStr.length(), whereIdx) );
		if ( fromArgs == null )
			throw new ParserException();

		tupleIterNames = new String[fromArgs.length];
		for ( int i = 0; i < fromArgs.length; i++ ){

			int start = 0;
			int end = fromArgs[i].length() - 1;
			for ( ; end >= 0; end-- ) /* skip whitespaces at the end */
				if ( !Character.isWhitespace(fromArgs[i].charAt(end)) )
					break;
			++end;

			/* If there is a closing parentheses, then we should find the
			 * corresponding opening parentheses */
			if ( fromArgs[i].charAt(end - 1) == ')' ){

				/* The first character after all whitespaces should be an opening
				 * parentheses. Otherwise, this is an error */
				for ( start = 0; start < end; start++ )
					if ( !Character.isWhitespace( fromArgs[i].charAt(start)) )
						break;

				if ( fromArgs[i].charAt(start) != '(' )
					throw new ParserException();

			}else{
				/* If the character is a closing bracket, then it is not a tuple iterator */
				if ( fromArgs[i].charAt(end - 1) != ']' ){

					for ( start = end - 1; start >= 0; start--)
						if ( Character.isWhitespace(fromArgs[i].charAt(start)) )
							break;
					start++;

					tupleIterNames[i] = fromArgs[i].substring(start, end);

					/* skip trailing whitespaces */
					for ( end = start - 1; end >= 0; end-- )
						if ( !Character.isWhitespace(fromArgs[i].charAt(end)) )
							break;
					++end;
				}else
					tupleIterNames[i] = "";

				for ( start = 0; start < end; start++ )
					if ( !Character.isWhitespace(fromArgs[i].charAt(start)) )
						break;
			}

			/* If we reached the start of the source and did not find any other
			 * characters, other than whitespaces, then the source did not have
			 * a tuple iterator name, and we have an anonymous tuple iterator */
			if ( start == end ){
				fromArgs[i] = tupleIterNames[i];
				tupleIterNames[i] = "";
			}else
				fromArgs[i] = fromArgs[i].substring(start, end);
		}

		if ( whereIdx != query.length() )
			parseWhereClause(query.substring(whereIdx + whereStr.length(), query.length()));

		/* TODO: The tuple iterators in the SELECT clause should exist in the FROM clause
		 * TODO: The same goes for any iterators in the WHERE clause */
	}

	/**
	 * This method is used to find an exact match of a string, i.e. searchFor, inside
	 * another string, i.e. inString. An exact match means that we can find the searchFor
	 * string as an individual term inside the inString.
	 * */
	private int findExactString( String searchFor, String inString, int whichMatch ){

		int idx = -1;
		
		while ( idx < 0 ){

			if ( whichMatch == FIRST_MATCH )
				idx = inString.indexOf(searchFor, idx + 1); /* Find where the searchFor string exists */
			else if ( whichMatch == LAST_MATCH )
				idx = inString.lastIndexOf(searchFor); /* Find where the searchFor string exists */
			
			if ( idx < 0 )
				return -1;

			/* If the string was found and both the previous and the next character are
			 * whitespaces, then we have an exact match */
			if ( ((idx == 0) || ( ( idx > 0 ) && (Character.isWhitespace( inString.charAt(idx - 1)))) ) && 
					( ((idx + searchFor.length()) == inString.length()) || 
							( ((idx + searchFor.length()) < inString.length()) &&
									Character.isWhitespace( inString.charAt(idx + searchFor.length() )) ) ) )
				return idx;

			/* Otherwise, keep looking */
			idx = idx + searchFor.length() - 1;
		}

		/* The index was not found, return -1 */
		return -1;
	}


	/**
	 * This method is used to parse the FROM clause into the
	 * distinct sources that are used.
	 * 
	 * @param fromClause: A string with all the sources that are in the FROM clause
	 * of the query.
	 * @return An array where each item is a separate source. In case of an error,
	 * e.g. the input is malformed (wrong parentheses) a null value is returned.
	 * @throws ParserException 
	 * */
	private String[] parseFromClause( String fromClause ) throws ParserException{

		List<String> sources = new ArrayList<String>();
		int start = 0, end = 0;

		int parSum = 0;
		while ( end < fromClause.length() ){

			switch( fromClause.charAt(end) ){
			case ',':

				/* You can only split if the parSum is 0 */
				if ( parSum == 0 ){
					sources.add( fromClause.substring(start, end) );
					start = end + 1;
				}
				break;
			case '(':
				++parSum;
				break;
			case ')':
				--parSum;
				break;
			}

			++end;
		}

		/* If at the end of the loop the parSum is not 0, then the
		 * query is malformed. In that case we return a null value
		 * instead of throwing an exception */
		if ( parSum != 0 ){
			sources.clear();
			sources = null;
			return null;
		}

		sources.add( fromClause.substring(start, end) );
		return (String[])sources.toArray(new String[0]);
	}

	/** Parses the given where  */
	private void parseWhereClause( String whereClause ){

		List<String> argCons = new LinkedList<String>();
		List<String> args = new LinkedList<String>();

		int con = 0;
		int idx = 0, tmpIdx;

		int start = 0;
		while ( start < whereClause.length() ){

			/* Find which of the tokens exists first */
			con = 0;
			idx = findTokenIndex(whereClause.substring(start), "and");

			if ( (tmpIdx = findTokenIndex(whereClause.substring(start), "or")) < idx ){
				con = 1;
				idx = tmpIdx;
			}

			if ( (tmpIdx = findTokenIndex(whereClause.substring(start), "not")) < idx ){
				con = 2;
				idx = tmpIdx;
			}

			args.add(whereClause.substring(start, start + idx).trim());

			if ( (start + idx) >= whereClause.length() )
				break;

			if ( con == 0 ){
				start += idx + 3;
				argCons.add("and");
			}else if ( con == 1 ){
				start += idx + 2;
				argCons.add("or");
			}else{
				start += idx + 3;
				argCons.add("not");
			}
		}

		whereArgs = args.toArray(new String[0]);
		args.clear();
		whereCons = argCons.toArray(new String[0]);
		argCons.clear();
		args = argCons = null;
	}

	private int findTokenIndex( String search, String token ){

		int idx = search.indexOf(token);
		if ( idx < 0 )
			return search.length();

		if ( idx != 0 && !( Character.isWhitespace(search.charAt(idx - 1)) ||
				(search.charAt(idx - 1) == ')') ) )
			return search.length();

		if ( (idx + token.length() < search.length()) && 
				( Character.isWhitespace( search.charAt(idx + token.length()) ) ||
						(search.charAt(idx + token.length()) == '(') ) )
			return idx;

		return search.length();
	}
		
	public String getPrefix(){
		return prefix;
	}

	public String[] getSelectArgs(){
		return selectArgs;
	}

	public String[] getFromArgs(){
		return fromArgs;
	}

	public String[] getTupleIterators(){
		return tupleIterNames;
	}

	public String[] getWhereArgs(){
		return whereArgs;
	}

	public String[] getWhereConditions(){
		return whereCons;
	}

	public String getSuffix(){
		return suffix;
	}

	public String getSelectClause(){

		StringBuilder sb = new StringBuilder();

		sb.append(selectStr);
		for ( int i = 0; i < selectArgs.length; i++ )
			sb.append(" ").append( selectArgs[i] ).append(",");
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public String getFromClause(){
		StringBuilder sb = new StringBuilder();

		sb.append(fromStr);
		for ( int i = 0; i < fromArgs.length; i++ )
			sb.append(" ").append( fromArgs[i] ).append( 
					(!tupleIterNames[i].isEmpty() ? " " : "" )).append(
							tupleIterNames[i]).append(",");

		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public String getWhereClause(){
		StringBuilder sb = new StringBuilder();

		if ( whereArgs != null ){
			sb.append(whereStr).append(' ');
			for ( int i = 0; i < whereArgs.length - 1; i++ )
				sb.append( whereArgs[i] ).append(' ').append(whereCons[i]).append(' ');
			sb.append(whereArgs[whereArgs.length - 1]);
		}

		return sb.toString();
	}

	public String toString(){
		StringBuilder sb = new StringBuilder();

		sb.append(prefix).append( getSelectClause() ).append(' ').append(
				getFromClause()).append(' ').append( getWhereClause() ).append(suffix);

		return sb.toString();
	}
}
