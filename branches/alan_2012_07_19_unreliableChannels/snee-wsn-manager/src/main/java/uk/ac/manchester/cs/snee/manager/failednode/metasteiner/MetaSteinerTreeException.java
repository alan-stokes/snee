package uk.ac.manchester.cs.snee.manager.failednode.metasteiner;

@SuppressWarnings("serial")
public class MetaSteinerTreeException extends Exception 
{

  public MetaSteinerTreeException(String message) 
  {
    super(message);
  }
  
  public MetaSteinerTreeException(String message, Throwable e) 
  {
    super(message);
  }
  
  public MetaSteinerTreeException(Throwable e) 
  {
    super(e);
  }
}
