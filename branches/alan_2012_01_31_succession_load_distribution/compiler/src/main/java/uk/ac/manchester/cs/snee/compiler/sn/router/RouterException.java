package uk.ac.manchester.cs.snee.compiler.sn.router;

@SuppressWarnings("serial")
public class RouterException extends Exception 
{
  public RouterException(String message) 
  {
    super(message);
  }
  
  public RouterException(String message, Throwable e) 
  {
    super(message);
  }
  
  public RouterException(Throwable e) 
  {
    super(e);
  }
}
