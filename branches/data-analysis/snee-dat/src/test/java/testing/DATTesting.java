package testing;

import gr.uoa.di.ssg4e.query.QueryRefactorer;
import gr.uoa.di.ssg4e.query.SNEEqlQuery;

import java.io.StringReader;
import java.util.Arrays;

import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import antlr.collections.AST;

public class DATTesting {

	public static void main( String[] args ){

//		System.out.append( "matches? " + "  f.temperature ".matches("[ ]*\\b.+\\..+\\b[ ]*"));
//		System.exit(-1);

		try{

			System.out.println("Query Refactoring Approach - NKUA v2.4 ");
			System.out.println("=======================================");

			int qi = 0;
			String q = null;
			SNEEqlQuery query = null;

			++qi;
			q = Queries.q1;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q2;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q3;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q4;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q5;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q6;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q7;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q8;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q9;
			System.out.println("Query " + qi + "    : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q10;
			System.out.println("Query " + qi + "   : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q11;
			System.out.println("Query " + qi + "   : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q12;
			System.out.println("Query " + qi + "   : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

			++qi;
			q = Queries.q13;
			System.out.println("Query " + qi + "   : " + q);
			System.out.println("Refactored : " + QueryRefactorer.refactorQuery(q).toString());
//			printQuery(query);
			System.out.println("================================================================");
			System.out.println();

		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public static AST doParsing( String query ){
		try {
			SNEEqlParser parser = new SNEEqlParser(new SNEEqlLexer(new StringReader(query)));

			parser.parse(); //Parse() substitutes the startRule() commonly found in documentation

			return parser.getAST();

		} catch (RecognitionException e) {
			e.printStackTrace();
		} catch (TokenStreamException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void printQuery ( SNEEqlQuery q ){
		System.out.println("SELECT args: " + Arrays.asList(q.getSelectArgs()).toString() );
		System.out.println("FROM args: " );
		for ( int i = 0; i < q.getFromArgs().length; i++ )
			System.out.println("\tIdentifier " + q.getTupleIterators()[i] + " iterates source " + q.getFromArgs()[i]);
		System.out.println("WHERE args: " + ((q.getWhereArgs() != null) ?
				Arrays.asList(q.getWhereArgs()).toString() : "") );
	}
}
