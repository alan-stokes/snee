package uk.ac.manchester.cs.snee.compiler.sn.router;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyUtils;

public class Router {

	/**
	 * Logger for this class.
	 */
	private static final Logger logger = Logger.getLogger(Router.class.getName());
	
	/**
	 * Random seed used by Steiner tree algorithm.
	 */
	int randomSeed = 4;
	PAF paf;

	/**
	 * Constructor for Sensor Network Routing Decision Maker.
	 * @throws NumberFormatException
	 * @throws SNEEConfigurationException
	 */
	public Router() throws NumberFormatException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER Router()");
		this.randomSeed= new Integer(SNEEProperties.getSetting(
				SNEEPropertyNames.ROUTER_RANDOM_SEED)).intValue(); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN Router()");
	}
	
	/**
	 * Carry out the sensor network routing.
	 * @param paf
	 * @param queryName
	 * @return
	 * @throws RouterException 
	 */
	public RT doRouting(PAF paf, String queryName) 
	throws RouterException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER doRouting() with " + paf.getID());
		//XXX: There is potentially one routing tree for each Sensor Network Source
		//For now, assume only one source
		SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) 
			paf.getDLAF().getSources().iterator().next();
		Topology network = sm.getTopology();
		outputTopology(network, queryName);
		int sink = sm.getGateway(); 
		int[] sources = sm.getSourceSites(paf);
		this.paf = paf;
		Tree steinerTree = computeSteinerTree(network, sink, sources); 
		RT rt = new RT(paf, queryName, steinerTree, network);
		if (logger.isDebugEnabled())
			logger.debug("RETURN doRouting()");
		return rt;
	}
	
	/**
	 * used to output the topology to the output folder
	 * @param network
	 */
	private void outputTopology(Topology network, String queryName)
  {
	  try
    {
      String sep = System.getProperty("file.separator");
	    String generalOutputFolder = SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR);
      String queryid = queryName + sep + "query-plan";
      String outputFolder = generalOutputFolder + sep + queryid;
      new TopologyUtils(network).exportAsDOTFile(outputFolder + sep + "defaultTopology", true);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
	 * used by the global and partial frameworks for partial optimisation
	 * @param paf
	 * @param queryName
	 * @param network
	 * @param _metadataManager 
	 * @return
	 * @throws RouterException 
	 */
	public RT doRouting(PAF paf, String queryName, Topology network, 
	                    SourceMetadataAbstract _metadataManager) 
	throws 
	RouterException 
	{
	 if (logger.isDebugEnabled())
	    logger.debug("ENTER doRouting() with " + paf.getID());
	    //XXX: There is potentially one routing tree for each Sensor Network Source
	    //For now, assume only one source
	    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadataManager;
	    int sink = sm.getGateway(); 
	    int[] sources = sm.getSourceSites(paf);
	    this.paf = paf;
	    Tree steinerTree = computeSteinerTree(network, sink, sources); 
	    RT rt = new RT(paf, queryName, steinerTree, network);
	    if (logger.isDebugEnabled())
	      logger.debug("RETURN doRouting()");
	    return rt;
	  }

	
	/**
	 * covering method to allow other code to work without being effected.
	 * @param network
	 * @param sink
	 * @param sources
	 * @return
	 * @throws RouterException 
	 */
	protected Tree computeSteinerTree(Topology network, final int sink, 
      final int[] sources) 
	throws 
	RouterException 
	{
	  return computeSteinerTree(network, sink, sources, true);
	}
	
	
    /**
     * Returns a graph representing the Steiner tree of the current graph.
     * @param sink 		the root of the graph
     * @param sources 	the vertices which are required to be in the steiner 
     * 					tree (known as Steiner nodes)
     * This is a non-deterministic algorithm and the result given depends 
     * on the order that the vertices given in the sources array are added to 
     * the Steiner tree. 
     * Simple algorithm used taken from "Protocols and Architectures from 
     * Wireless Sensor Networks" by Holger Karl and Andreas Willig,
     * page 309. 
     * @throws RouterException 
     */
    protected Tree computeSteinerTree(Topology network, final int sink, 
    		final int[] sources, boolean setUpSources) 
    throws 
    RouterException 
    {
		if (logger.isTraceEnabled())
			logger.trace("ENTER computeSteinerTree() with sink=" +sink+
					" sources="+sources.toString());    	
    	Site gateway = new Site(network.getSite(sink));
    	Tree steinerTree = new Tree(gateway, true);

		final ArrayList<String> nodesToAdd = new ArrayList<String>();
		final ArrayList<String> nodesAdded = new ArrayList<String>();
	
		for (int element : sources) {
		    nodesToAdd.add(new Integer(element).toString());
		}
		boolean first = true;
		final Random random = new Random(this.randomSeed);
		
    if(setUpSources)
    {
		//TODO: bug remains, sink of first routing tree never gets set as source...
	    Iterator<Node> siteIter = network.siteIterator();
	    while (siteIter.hasNext()) {
	    	Site s = (Site) siteIter.next();
	    	if (nodesToAdd.contains(s.getID())) {
	    		s.setIsSource(true);
	    	}
	    	if (gateway.getID().equals(s.getID())) {
	    		gateway.setIsSource(true);
	    	}
	    }
    }
	
		while (nodesToAdd.size() > 0) {
		    //get a source node at random
		    final int randomPos = random.nextInt(nodesToAdd.size());
		    final String sourceNodeID = nodesToAdd.get(randomPos);
		    String destNode = null;
		    nodesToAdd.remove(randomPos);
	
		    LinkCostMetric linkCostMetric = LinkCostMetric.ENERGY;
		    Path shortestPath;
		    if (first) {
				//the first time, get the path from the currentSource to the sink
				shortestPath = network.getShortestPath(sourceNodeID, new Integer(
					sink).toString(), linkCostMetric);
				destNode = new Integer(sink).toString();
				nodesAdded.add(new Integer(sink).toString());
				first = false;
	
		    } else {
				//add path from source node to a random destination node added to the steiner tree 
				destNode = nodesAdded.get(random
					.nextInt(nodesAdded.size()));
				shortestPath = network.getShortestPath(sourceNodeID, destNode, 
						linkCostMetric);
		    }
		    
		    if(shortestPath == null)
		      throw new RouterException("no route between nodes " + sourceNodeID + " and " + destNode);
		    //now traverse shortest path from the currentSource, adding edges to the steiner tree,
		    //until you find a node already in the steiner tree
		    String tmpPrev = sourceNodeID;
		    String tmpCurrent = sourceNodeID;
		    boolean foundFlag = false;
	
		    if (steinerTree.getNode(sourceNodeID)==null) {
		    	Iterator<Site> pathIter = shortestPath.iterator();
		    	while (pathIter.hasNext() && !foundFlag) {
				    tmpPrev = tmpCurrent;
				    tmpCurrent = pathIter.next().getID();
		
				    if (steinerTree.getNode(tmpCurrent)!=null) {
				    	foundFlag = true;
				    }
				    if (tmpPrev.equals(tmpCurrent)) {
				    	continue;
				    }
		    	    addTreeLink(steinerTree, network.getSite(tmpPrev), 
		    	    		network.getSite(tmpCurrent));
				    
				    logger.trace("Added edge to steiner tree:" + tmpPrev + "-"
					    + tmpCurrent);
		    	}
			}
			nodesAdded.add(sourceNodeID.toString());
		}
		steinerTree.updateNodesAndEdgesColls(gateway);
		if (logger.isTraceEnabled())
			logger.trace("RETURN computeSteinerTree()");
		return steinerTree;
    }
    
    /**
     * Copies a radio link from another topology, and adds it to this one.
     * @param oldSource the source site
     * @param oldDest the destination site
     * @return the edge which was created
     */
    private final void addTreeLink(Tree rt,
    		final Site oldSource, final Site oldDest) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER Router()");
    	Site source = (Site) rt.getNode(oldSource.getID());
    	if (source==null) {
        	source = new Site(oldSource);
        	rt.addNode(source);
    	}
    	Site dest = (Site) rt.getNode(oldDest.getID());
    	if (dest==null) {
        	dest= new Site(oldDest);
    		rt.addNode(dest);
    	}
    	source.addOutput(dest);
    	dest.addInput(source);
		if (logger.isTraceEnabled())
			logger.trace("RETURN Router()");
    }
}
