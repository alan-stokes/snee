package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class LogicalOverlayNetworkUtils
{

  private String sep = System.getProperty("file.separator");
  
  public LogicalOverlayNetworkUtils()
  {
    
  }
  
  
  public void storeOverlayAsFile(LogicalOverlayNetwork overlay, File outputFile) 
  throws FileNotFoundException, IOException
  {
    if(!outputFile.exists())
      outputFile.mkdir();
    ObjectOutputStream outputStream = 
      new ObjectOutputStream(new FileOutputStream(outputFile.toString() + sep + overlay.getId()));
    outputStream.writeObject(overlay);
    outputStream.flush();
    outputStream.close();
  }
  
  public LogicalOverlayNetwork retrieveOverlayFromFile(File outputFile, String id) 
  throws IOException
  {
    ObjectInputStream inputStream = null;
    File file = new File(outputFile + sep + id);
    inputStream = new ObjectInputStream(new FileInputStream(file));
        
    Object obj = null;
    //try reading in object
    try
    {
      obj = inputStream.readObject();
    }
    catch (ClassNotFoundException e)
    {
     throw new IOException(e.getLocalizedMessage());
    }
    
    //if its of the correct format, return overlay
    if (obj instanceof LogicalOverlayNetwork) 
    {
      LogicalOverlayNetwork overlay = (LogicalOverlayNetwork) obj;
      return overlay;
    }   
    return null;
  }
  
  
}
