package gr.uoa.di.ssg4e.query;

import gr.uoa.di.ssg4e.dat.DataAnalysisTechnique;
import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.dat.schema.DATMetadata;
import gr.uoa.di.ssg4e.query.excep.ParserException;

/**
 * Class that implements the Query Refactorer object. The item takes as 
 * input the metadata of existing extents.
 * */
public class QueryRefactorer {

	/* The controller used by this refactoring object */
	private IMetadata _metadata = null;

	/**
	 * Constructor for a new query refactoring object
	 * */
	public QueryRefactorer( IMetadata metadata ){
		_metadata = metadata;
	}

	/** 
	 * This method performs preprocessing on the given query so that any Data Analysis Techniques
	 * (DATs) are transformed to their SQL-like equivalent counterparts. Everything is performed
	 * using string substitution at this point.
	 * 
	 * Queries that we want to refactor are only SELECT-FROM-WHERE clauses. These are the only
	 * cases that we need to take into account.
	 * 
	 * @param query: A SQL query on which we want to perform the preprocessing. The query has not
	 * been parsed so it may not be valid, however we do not care about it at the moment
	 *  
	 * @throws ParserException in case a parsing error occurs
	 * @throws DATException if the DAT that is used
	 * @throws ExtentDoesNotExistException 
	 *  */
	public String refactorQuery( String query ) 
	throws ParserException, DATException{

		/*
		 * FIXME: Known Limitations: UNION can only be top level.
		 * According to the test cases, it is not supported any other way than being top-level 
		 * */

		StringBuilder gsb = new StringBuilder(query.length());
		query = query.toLowerCase();

		/* SPLIT the queries on "UNION", which can only exist at the top level */
		String[] queries = query.split("union");

		StringBuilder lsb = null;
		for ( int i = 0; i < queries.length; i++ ){

			/* Create a local string builder where the result of this query will be given */
			lsb = new StringBuilder(queries[i].length());
			refactorQuery( new SNEEqlQuery(queries[i]), lsb );
			queries[i] = null;

			gsb.append(lsb.toString());
			lsb = null;

			gsb.append("union");
		}

		queries = null;
		query = gsb.substring(0, gsb.length() - "union".length());
		gsb = null;
		return query;
	}


	/** 
	 * This method performs preprocessing on the given query so that any Data Analysis Techniques
	 * (DATs) are transformed to their SQL-like equivalent counterparts. Everything is performed
	 * using string substitution at this point.
	 * 
	 * Queries that we want to refactor are only SELECT-FROM-WHERE clauses. These are the only
	 * cases that we need to take into account.
	 * 
	 * @param query: A SQL query on which we want to perform the preprocessing. The query has not
	 * been parsed so it may not be valid, however we do not care about it at the moment
	 *  
	 * @throws ParserException in case a parsing error of the query occurs
	 * @throws DATException if the DAT that is used
	 *  */
	private void refactorQuery( SNEEqlQuery query, StringBuilder sb ) 
	throws ParserException, DATException {

		if ( query == null )
			return;

		/* At this point we know:
		 * 1) The SELECT clause starts at <selectIdx> and ends at <fromIdx>
		 * 2) The FROM clause starts at <fromIdx> and ends at <whereIdx>
		 * 3) The WHERE clause, if present, starts at whereIdx.
		 *    The where clause is present if whereidx != query.length()
		 *  */

		int tmpLen = sb.length();

		/* Split the FROM clause to see if a DAT is used. The FROM clause is split on commas */
		String[] sources = query.getFromArgs();
		int i = 0;
		for ( ; i < sources.length; i++ ){

			/* For each source, check if it is a sub-query, by asking for a SELECT clause */
			if ( sources[i].indexOf(SNEEqlQuery.selectStr) >= 0 ){

				/* The source contains a subquery. Subqueries are not checked for DATs
				 * at this point. They are managed as SNEEqlQueries and are added
				 * recursively by calling the refactoring method */
				sb.append(' ');
				refactorQuery(new SNEEqlQuery(sources[i]), sb);
				sb.append(' ').append(query.getTupleIterators()[i]);

			}else{

				/* Get the source name */
				String src = sources[i].trim();
				int idx = src.indexOf('[');
				if ( idx < 0 ){
					/* Since the opening bracket does not exist, go to the end */
					for ( idx = src.length() - 1; idx >= 0; idx-- )
						if ( Character.isWhitespace(src.charAt(idx)) )
							break;
				}

				/* No whitespaces, the src is the entire source[i]. Otherwise
				 * it is the  */
				if ( idx <= 0 )
					idx = src.length();
				src = src.substring(0, idx).trim();


				/* The source is not a subquery. Get the metadata for this source */
				IExtentMetadata metadata = _metadata.getExtentMetadata(src);

				/* If it is a Data Analysis Technique, i.e. an intentional extent
				 * FIXME: We should only try and refactor if we know that the DAT is
				 * refactorable */
				if ( metadata.isIntensional() ){

					DATMetadata datMeta = metadata.getDATMetadata();

					/* Load the appropriate DAT, given the DAT metadata */
					DataAnalysisTechnique dat = DataAnalysisTechnique.loadDAT( sources[i], datMeta );

					/* There can be only one DAT FIXME: Is this correct???
					 * If the DAT is found, we do not append. We directly add the
					 * refactored query to sb */
					sb.replace(tmpLen, sb.length(), dat.refactorQuery( query, i ));
					sources = null;
					return;
				}

				sb.append(sources[i]);
				if ( !query.getTupleIterators()[i].isEmpty() )
					sb.append(' ').append(query.getTupleIterators()[i]);
			}

			sb.append(",");
		}

		sb.deleteCharAt(sb.length() - 1);

		/* if there is not a DAT in the FROM clause, then the query,
		 * excluding sub-queries, should remain untouched */
		if ( i == sources.length ){
			sb.insert(tmpLen, query.getPrefix() + query.getSelectClause() + ' ' + SNEEqlQuery.fromStr + ' ');

			if ( query.getWhereArgs() != null )
				sb.append(' ').append( query.getWhereClause() );
			
			sb.append(query.getSuffix() );
		}

		sources = null;
	}
}
