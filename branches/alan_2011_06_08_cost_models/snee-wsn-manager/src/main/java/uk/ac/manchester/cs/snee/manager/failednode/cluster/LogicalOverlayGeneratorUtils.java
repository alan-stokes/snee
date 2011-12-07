package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.File;
import java.io.FileInputStream;
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
  
  public void storeQEP(SensorNetworkQueryPlan qep, File localFolder) 
  throws IOException
  {
    ObjectOutputStream outputStream = 
      new ObjectOutputStream(new FileOutputStream(localFolder.toString() + sep + qep.getID()));
    outputStream.writeObject(qep);
    outputStream.flush();
    outputStream.close();
  }
  
}
