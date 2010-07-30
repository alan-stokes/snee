package uk.ac.manchester.cs.snee.compiler.translator;

public class ParserValidationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ParserValidationException(String text)
	{
		super(text);
	}

	public static double noDouble (String text) 
	throws ParserValidationException 
	{
		throw new ParserValidationException(text);
	}
}
