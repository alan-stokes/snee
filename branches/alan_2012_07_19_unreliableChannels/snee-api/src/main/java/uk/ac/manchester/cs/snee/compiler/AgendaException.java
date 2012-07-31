package uk.ac.manchester.cs.snee.compiler;

public class AgendaException extends Exception 
{
  /**
   * 
   */
  private static final long serialVersionUID = -161756648988251174L;

  /**
   * Construct a new optimisation exception with
   * the given message.
   */
  public AgendaException(final String message) 
  {
    super(message);   
  }

  /**
   * Construct a new optimisation exception with 
   * the given message and cause.
   */
  public AgendaException(final String message, final Throwable cause)
  {
    super(message, cause);   
  }

}
