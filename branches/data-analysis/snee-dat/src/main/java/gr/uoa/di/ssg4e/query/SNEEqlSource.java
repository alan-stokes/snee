/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

import gr.uoa.di.ssg4e.query.excep.ParserException;

/**
 * Class used to represent SNEE sources, which are low-level.
 */
public class SNEEqlSource {

	/** The name that is used to identify the  */
	private String sourceName = null;

	/** The window that the source extends over */
	private String sourceWindow = null;

	/** The iterator name used to identify */
	private String sourceIterator = null;

	/** The source string as it was given initially */
	private String src = null;

	/**
	 * Public constructor which takes the source 
	 * */
	public SNEEqlSource( String source ) throws ParserException {

		src = source;
		parse();

		/* If after parsing the window is unspecified, then use an empty window */
		if ( sourceWindow == null )
			sourceWindow = "";

		if ( sourceIterator == null )
			sourceIterator = "";
	}

	private void parse() throws ParserException{

		String source = src.trim();
		int idx = 0;
		int bracCnt = 0;

		/* Run over the source */
		for ( ; idx < source.length(); idx++ ){

			if ( source.charAt(idx) == '[' ){ /* opening bracket */

				bracCnt = 1;
				sourceName = source.substring(0, idx);
				break;

			}else if ( Character.isWhitespace(source.charAt(idx)) ){

				/* Character is a whitespace. The source name goes this far.
				 * Skip any additional whitespaces until the end of the string */
				sourceName = source.substring(0, idx);
				for ( ; idx < source.length(); idx++ )
					if ( !Character.isWhitespace(source.charAt(idx)) )
						break;

				/* Check if the position where we stopped is an opening bracket */
				if ( source.charAt(idx) == '[' )
					bracCnt = 1;
				break;
			}
		}

		/* We did not break the above loop. There is only the sourceName */
		if ( idx == source.length() ){
			sourceName = source;
			return;
		}

		/* Get the window, provided there is one */
		int startIdx = idx;
		while ( (bracCnt != 0) && (idx < source.length()) ){

			if ( source.charAt(idx) == '[' ) /* Increase bracket count when seeing a [ */
				++bracCnt;
			else if ( source.charAt(idx) == ']' ){

				/* Encountered closing bracket. Decrease the counter and check that closes
				 * all of the previous opened brackets */
				if ( --bracCnt == 0 ){
					sourceWindow = source.substring(startIdx, idx);
					break;
				}
			}

			++idx;
		}

		/* If the input was consumed and there are remaining opened brackets,
		 * return a parse error */
		if ( bracCnt != 0 )
			throw new ParserException();

		/* The opening bracket does not exist, go to the end,
		 * and move backwards until the first whitespace. */
		if ( idx != source.length() )
			sourceIterator = source.substring(idx, source.length()).trim();
	}

	/** Returns the source as it was given */
	public String toString(){
		return src;
	}

	public String getName(){
		return sourceName;
	}

	public String getWindow(){
		return sourceWindow;
	}

	public String getIterator(){
		return sourceIterator;
	}
}
