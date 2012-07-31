package uk.ac.manchester.cs.snee.compiler;

public class AgendaLengthException extends Exception 
{
  /**
   * 
   */
  private static final long serialVersionUID = -161756648988251174L;

  /**
   * Construct a new optimisation exception with
   * the given message.
   */
  public AgendaLengthException(final String message) 
  {
    super(message);   
  }

  /**
   * Construct a new optimisation exception with 
   * the given message and cause.
   */
  public AgendaLengthException(final String message, final Throwable cause)
  {
    super(message, cause);   
  }
}
