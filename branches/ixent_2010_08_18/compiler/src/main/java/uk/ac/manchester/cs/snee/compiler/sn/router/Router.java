package uk.ac.manchester.cs.snee.compiler.sn.router;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;

public class Router {

	private Logger logger = 
		Logger.getLogger(Router.class.getName());
	
	public Metadata metadata;
	
	int randomSeed = 4; //TODO: read this from settings
	
	public void RT(Metadata m) {
		metadata = m;
	}
	
	public RT doRouting(PAF paf, String queryName) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doRouting() with " + paf.getName());
		//XXX: There is potentially one routing tree for each Sensor Network Source
		//For now, assume only one source
		SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) 
			paf.getDLAF().getSource();
		Topology network = sm.getTopology();
		int sink = sm.getGateway(); 
		int[] sources = sm.getSourceSites();
		Tree steinerTree = computeSteinerTree(network, sink, sources); 
		RT rt = new RT(paf, queryName, steinerTree);
		if (logger.isTraceEnabled())
			logger.trace("RETURN doRouting()");
		return rt;
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
    public Tree computeSteinerTree(Topology network, final int sink, final int[] sources) {

    	Site gateway = new Site(network.getSite(sink));
    	Tree steinerTree = new Tree(gateway);

		final ArrayList<String> nodesToAdd = new ArrayList<String>();
		final ArrayList<String> nodesAdded = new ArrayList<String>();
	
		for (int element : sources) {
		    nodesToAdd.add(new Integer(element).toString());
		}
		boolean first = true;
		final Random random = new Random(this.randomSeed);

		//TODO: bug remains, sink of first routing tree never gets set as source...
	    Iterator<Node> siteIter = network.siteIterator();
	    while (siteIter.hasNext()) {
	    	Site s = (Site) siteIter.next();
	    	if (nodesToAdd.contains(s.getID())) {
	    		s.setIsSource(true);
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
				shortestPath = network.getShortestPath(sourceNodeID, destNode, linkCostMetric);
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
		
				    if (!tmpPrev.equals(tmpCurrent)) {
				    	String linkID = network.generateEdgeID(tmpPrev, tmpCurrent);
				    	RadioLink oldLink = (RadioLink) network.getEdge(linkID);
				    	addExternalSiteAndRadioLinkClone(steinerTree,
				    			network.getSite(tmpPrev), network.getSite(tmpCurrent), 
				    			oldLink);
				    }	
				    logger.trace("Added edge to steiner tree:" + tmpPrev + "-"
					    + tmpCurrent);
		    	}
			}
			nodesAdded.add(sourceNodeID.toString());
		}
		return steinerTree;
    }
    
    /**
     * Copies a radio link from another topology, and adds it to this one.
     * @param oldSource the source site
     * @param oldDest the destination site
     * @param oldLink the link to be copied
     * @return the edge which was created
     */
    public final RadioLink addExternalSiteAndRadioLinkClone(Tree rt,
    		final Site oldSource, final Site oldDest, 
    		final RadioLink oldLink) {
    	//clone the nodes
    	final Site newSource = new Site(oldSource);
    	final Site newDest = new Site(oldDest);
    	
    	//clone the radio link
    	RadioLink newLink = addRadioLink(rt, newSource.getID(), 
    			newDest.getID(), false, oldLink.getRadioLossCost());
    	newLink.setEnergyCost(oldLink.getEnergyCost());
    	newLink.setLatencyCost(oldLink.getLatencyCost());		
    	return newLink;
    }   
    
    public RadioLink addRadioLink(Tree rt, final String id1, final String id2,
    	    final boolean bidirectional, final double radioLossCost) {
        	RadioLink link = (RadioLink) rt.addEdge(id1, id2,
    		bidirectional);
        	link.setRadioLossCost(radioLossCost);
        	return link;
        }
}
