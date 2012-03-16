package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class FailedNodeLocalLogicalOverlayUtils
{
  private LogicalOverlayNetworkImpl cluster;
  private File outputFolder;
  private String sep = System.getProperty("file.separator");
  
  public FailedNodeLocalLogicalOverlayUtils(LogicalOverlayNetworkImpl cluster, File outputfolder)
  {
    this.cluster = cluster;
    this.outputFolder = outputfolder;
  }
  
  public String toString()
  {
    String output = "";
    Set<String> keyset = cluster.getKeySet();
    boolean firstKey = true;
    Iterator<String> keysetIterator = keyset.iterator();
    
    while(keysetIterator.hasNext())
    {
      String key = keysetIterator.next();
      if(firstKey)
      {
        output = output + "key " + key + " Nodes ";
        firstKey = false;
      }
      else
        output = output + "\nkey " + key + " Nodes ";
      
      ArrayList<String> equivilentNodes = cluster.getEquivilentNodes(key);
      Iterator<String> equivilentNodesIterator = equivilentNodes.iterator();
      while(equivilentNodesIterator.hasNext())
      {
        String node = equivilentNodesIterator.next();
        if(equivilentNodesIterator.hasNext())
          output = output + node + ", ";
        else
          output = output + node;
      }
    }
    return output;
  }
  
  public void outputAsTextFile() 
  throws IOException
  {
    String toString = this.toString();
    File clusterFolder = new File(outputFolder.toString() + sep + "Localclusters");
    clusterFolder.mkdir();
    File outputFile = new File(clusterFolder.toString() + sep + "nodeClusters");
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    writer.write(toString);
    writer.flush();
    writer.close();
    
  }
}
