package uk.ac.manchester.cs.snee.manager.common;

import java.io.File;
import java.io.Serializable;

import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;

public abstract class AutonomicManagerComponent implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5477565839619065406L;
  
  protected AutonomicManagerImpl manager;
  protected String sep = System.getProperty("file.separator");
  
  public void deleteFolder(File file)
  {
    manager.deleteFileContents(file);
  }
}
