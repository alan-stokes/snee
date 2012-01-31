package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;

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
  
  public void storeOverlayAsTextFile(LogicalOverlayNetwork overlay, File outputFile)
  throws FileNotFoundException, IOException
  {
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
    Iterator<String> keys = overlay.getKeySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      ArrayList<String> eqNodes = overlay.getEquivilentNodes(key);
      out.write("[" + key + "] : [ " + eqNodes.toString() + " ] ");
      out.newLine();
    }
    out.flush();
    out.close();
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


  public void storeSetAsTextFile(ArrayList<LogicalOverlayNetwork> setsOfClusters, File outputFile)
  throws IOException
  {
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
    Iterator<LogicalOverlayNetwork> iterator = setsOfClusters.iterator();
    int id = 1;
    while(iterator.hasNext())
    {
      LogicalOverlayNetwork overlay = iterator.next();
      Iterator<String> keys = overlay.getKeySet().iterator();
      out.write(new Integer(id).toString());
      while(keys.hasNext())
      {
        String key = keys.next();
        ArrayList<String> eqNodes = overlay.getEquivilentNodes(key);
        out.write(" [" + key + "] : [ " + eqNodes.toString() + " ] ");
        out.newLine();
      }
      out.newLine();
      id++;
    }
    out.flush();
    out.close();
    
  }
  
  
}
