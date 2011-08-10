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
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class Router {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(Router.class.getName());
	
	/**
	 * Random seed used by Steiner tree algorithm.
	 */
	int randomSeed = 4;

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
	 */
	public RT doRouting(PAF paf, String queryName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doRouting() with " + paf.getID());
		//XXX: There is potentially one routing tree for each Sensor Network Source
		//For now, assume only one source
		SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) 
			paf.getDLAF().getSources().iterator().next();
		Topology network = sm.getTopology();
		int sink = sm.getGateway(); 
		int[] sources = sm.getSourceSites(paf);
		Tree steinerTree = computeSteinerTree(network, sink, sources); 
		RT rt = new RT(paf, queryName, steinerTree);
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
	 */
	protected Tree computeSteinerTree(Topology network, final int sink, 
      final int[] sources) 
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
     */
    protected Tree computeSteinerTree(Topology network, final int sink, 
    		final int[] sources, boolean setUpSources) {
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
		    nodesToAdd.remove(randomPos);
	
		    LinkCostMetric linkCostMetric = LinkCostMetric.RADIO_LOSS;
		    Path shortestPath;
		    if (first) {
				//the first time, get the path from the currentSource to the sink
				shortestPath = network.getShortestPath(sourceNodeID, new Integer(
					sink).toString(), linkCostMetric);
				nodesAdded.add(new Integer(sink).toString());
				first = false;
	
		    } else {
				//add path from source node to a random destination node added to the steiner tree 
				final String destNode = nodesAdded.get(random
					.nextInt(nodesAdded.size()));
				shortestPath = network.getShortestPath(sourceNodeID, destNode, 
						linkCostMetric);
		    }
		    
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
