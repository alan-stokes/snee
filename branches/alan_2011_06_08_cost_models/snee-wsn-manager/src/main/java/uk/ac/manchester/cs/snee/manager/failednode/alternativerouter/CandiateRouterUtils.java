package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class CandiateRouterUtils
{

  private Topology network;
  private String sep = System.getProperty("file.separator");
  
  public CandiateRouterUtils(Topology network)
  {
    this.network = network;
  }
  
  
  
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



  public void exportRouteFragments(File desintatedOutputFolder, ArrayList<Tree> routes, PAF paf, 
                                   Tree steinerTree)
  {
    new RTUtils(new RT(paf, "", steinerTree, network)).
    exportAsDotFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1)); 
    new RTUtils(new RT(paf, "", steinerTree, network)).
    exportAsTextFile(desintatedOutputFolder.toString() + sep + "firstroute" + (routes.size() + 1));
  }
  
}
