package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyUtils;

public class CandiateRouterUtils
{

  private Topology network;
  private String sep = System.getProperty("file.separator");
  
  public CandiateRouterUtils(Topology network)
  {
    this.network = network;
  }
  
  
  /**
   * generates the reduced routes as new list
   * @param routes
   * @param outputFolder
   * @param paf
   */
  public void exportSavedRoutes(ArrayList<Tree> routes, File outputFolder, PAF paf)
  {
    Iterator<Tree> routeIterator = routes.iterator();
    File cleaned = new File(outputFolder.toString() + sep + "reducedPossible");
    cleaned.mkdir();
    int counter = 0;
    while(routeIterator.hasNext())
    {
      Tree currentTree = routeIterator.next();
      new RTUtils(new RT(paf, "", currentTree, network)).
            exportAsDotFile(cleaned.toString() + sep + "route" + counter); 
      new RTUtils(new RT(paf, "", currentTree, network)).
            exportAsTextFile(cleaned.toString() + sep + "route" + counter); 
      counter++;
    }
  }
  
  /**
   * outputs all routing trees to autonomic folder
   * @param newRoutingTrees
   * @param outputFolder 
   */
  public void exportCompleteTrees(ArrayList<RT> newRoutingTrees, File outputFolder)
  {
    File output = new File(outputFolder.toString() + sep + "completeRoutingTrees");
    output.mkdir();
    Iterator<RT> routes = newRoutingTrees.iterator();
    int counter = 1;
    while(routes.hasNext())
    {
      RT route = routes.next();
      new RTUtils(route).exportAsDotFile(output.toString() + sep + "completeRoute" + counter);
      new RTUtils(route).exportAsTextFile(output.toString() + sep + "completeRoute" + counter);
      counter ++;
    }
    
  }
  
  /**
   * generates the route fragments as dot files
   * @param desintatedOutputFolder
   * @param routes
   * @param paf
   * @param steinerTree
   */
  public void exportRouteFragments(File desintatedOutputFolder, ArrayList<Tree> routes, PAF paf, 
                                   Tree steinerTree)
  {
    new RTUtils(new RT(paf, "", steinerTree, network)).
    exportAsDotFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1)); 
    new RTUtils(new RT(paf, "", steinerTree, network)).
    exportAsTextFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1));
  }


  /**
   * outputs a topology as a dot file (named reduced)
   * @param chainFolder
   * @param labels
   * @param workingTopology 
   * @throws SchemaMetadataException 
   */
  public void exportReducedTopology(File desintatedOutputFolder, boolean labels, 
                                    Topology workingTopology) 
  throws SchemaMetadataException
  {
    new TopologyUtils(workingTopology).exportAsDOTFile(desintatedOutputFolder.toString() + sep + 
        "reducedtopology" , labels);
    
  }

  /**
   * outputs file recording which nodes have been depinned
   * @param depinnedNodes
   * @param desintatedOutputFolder
   * @throws IOException 
   */
  public void exportDepinnedNodes(ArrayList<String> depinnedNodes, File desintatedOutputFolder) 
  throws IOException
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(
        new File(desintatedOutputFolder.toString() + sep + "depinnedNodes")));
    writer.write(depinnedNodes.toString());
    writer.flush();
    writer.close();
  }
  
  public void exportTempRoutingTopology(File desintatedOutputFolder, String fileName, 
                                        boolean labels, Topology tempTop) 
  throws SchemaMetadataException
  {
    if(!desintatedOutputFolder.exists())
      desintatedOutputFolder.mkdir();
    new TopologyUtils(tempTop).exportAsDOTFile(desintatedOutputFolder.toString() + sep + fileName, labels);
  }
  
}
