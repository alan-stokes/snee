package uk.ac.manchester.cs.snee.sncb;

public class CodeGenerationException extends Exception {

  /**
 * 
 */
private static final long serialVersionUID = 1L;

public CodeGenerationException(final String msg) {
super(msg);
  }

public CodeGenerationException(String msg, Exception e) {
  super(msg, e);
}


public CodeGenerationException(Exception e) {
  super(e.getLocalizedMessage(), e);
}
}