package uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class LogicalOverlayGeneratorUtils
{

  private String sep = System.getProperty("file.separator"); 
  public LogicalOverlayGeneratorUtils()
  {}
  
  /**
   * extracts a SensorNetworkQueryPlan with a specific id from a file into a 
   * SensorNetworkQueryPlan object
   * @param localFolder
   * @param qepID
   * @return
   * @throws IOException
   */
  public SensorNetworkQueryPlan retrieveQEP(File localFolder, String qepID) 
  throws IOException
  {
    ObjectInputStream inputStream = null;
    File file = new File(localFolder + sep + qepID);
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
    if (obj instanceof SensorNetworkQueryPlan) 
    {
      SensorNetworkQueryPlan qep = (SensorNetworkQueryPlan) obj;
      return qep;
    }   
    return null;
  }
  
  
  /**
   * stores a SensorNetworkQueryPlan in a file with its specfic id.
   * @param qep
   * @param localFolder
   * @throws IOException
   */
  public void storeQEP(SensorNetworkQueryPlan qep, File localFolder) 
  throws IOException
  {
    ObjectOutputStream outputStream = 
      new ObjectOutputStream(new FileOutputStream(localFolder.toString() + sep + qep.getID()));
    outputStream.writeObject(qep);
    outputStream.flush();
    outputStream.close();
  }

  /**
   * stores a overlay into a text file (easily readable)
   * @param logicalOverlay
   * @param localFolder
   * @throws FileNotFoundException
   * @throws IOException
   */
  public void storeOverlayAsText(LogicalOverlayNetwork logicalOverlay, File localFolder) 
  throws FileNotFoundException, IOException
  {
    new LogicalOverlayNetworkUtils().storeOverlayAsTextFile(logicalOverlay, 
                                   new File(localFolder.toString() + sep + logicalOverlay.getId()));
  }
  
}
