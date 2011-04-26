package uk.ac.manchester.cs.snee.compiler.parser;

public class ParserValidationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4581810935014008277L;

	public ParserValidationException(String text)
	{
		super(text);
	}
	
	public static double noDouble (String text) throws ParserValidationException 
	{
		throw new ParserValidationException(text);
	}
}
