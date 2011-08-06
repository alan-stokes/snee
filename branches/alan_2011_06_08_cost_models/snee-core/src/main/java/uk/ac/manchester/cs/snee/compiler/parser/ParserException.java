package uk.ac.manchester.cs.snee.compiler.parser;

public class ParserException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1536905569333420581L;

	public ParserException(String text)
	{
		super(text);
	}
	
	public ParserException(String msg, Throwable ex) {
		super(msg, ex);
	}

	public static double noDouble (String text) throws ParserException 
	{
		throw new ParserException(text);
	}
}
