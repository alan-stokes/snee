package uk.ac.manchester.cs.snee.manager.common;

import java.io.File;
import java.io.Serializable;

import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public abstract class AutonomicManagerComponent implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5477565839619065406L;
  
  protected AutonomicManagerImpl manager;
  protected SourceMetadataAbstract _metadata;
  protected String sep = System.getProperty("file.separator");
  
  public void deleteFolder(File file)
  {
    manager.deleteFileContents(file);
  }
}
