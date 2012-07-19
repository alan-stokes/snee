package uk.ac.manchester.cs.snee.manager;

@SuppressWarnings("serial")
public class AutonomicManagerException extends Exception
{
  public AutonomicManagerException(String message) 
  {
    super(message);
  }
  
  public AutonomicManagerException(String message, Throwable e) 
  {
    super(message);
  }
  
  public AutonomicManagerException(Throwable e) 
  {
    super(e);
  }
}
