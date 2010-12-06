/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package testing;


import java.io.StringReader;

import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;

import antlr.RecognitionException;
import antlr.TokenStreamException;

public class ParserTesting {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String query = "CREATE CLASSIFIER [linearRegressionFunction, pressure] forestLRF " +
				"FROM forest[FROM NOW - 20 MIN TO NOW];";

//		query = "SELECT F.temperature AS temp " +
//				"FROM forest[NOW] F, forestPF FPF " +
//				"WHERE F.temperature = FPF.temperature AND F.pressure = FPF.pressure AND FPF.probability < 0.05;";

		try {
			SNEEqlParser parser = new SNEEqlParser(new SNEEqlLexer(new StringReader(query)));

			parser.parse(); //Parse() substitutes the startRule() commonly found in documentation

			parser.getAST();

		} catch (RecognitionException e) {
			e.printStackTrace();
		} catch (TokenStreamException e) {
			e.printStackTrace();
		}

	}
}
