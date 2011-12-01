/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.metadata.source.sensornet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


import org.apache.log4j.Logger;


import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.Graph;
import uk.ac.manchester.cs.snee.common.graph.Node;


/**
 * Represents a network connectivity graph for a sensor network.
 *
 */
public class Topology extends Graph {

    /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2381466773289438786L;
    private static final Logger logger = Logger.getLogger(Topology.class.getName());

    /** 	
     * Constructor.
     * @param name 		the name of the graph
     * @param directed 	whether the graph should have directed edges or not
     * 
     */
    public Topology(final String name, final boolean directed) {
	super(name, directed, false);
    }

    @Override
    public Site addNode(final String id) {
	final Site newNode = new Site(id);
	return (Site) super.addNode(newNode);
    }

    public Site getSite(final String id) {
	return (Site) this.getNode(id);
    }

    public Site getSite(final int id) {
	return (Site) this.getNode(id);
    }

    public RadioLink addRadioLink(final String id1, final String id2,
	    final boolean bidirectional, final double radioLossCost) {
    	final RadioLink link = (RadioLink) super.addEdge(id1, id2,
		bidirectional);
    	link.setRadioLossCost(radioLossCost);
    	return link;
    }

    @Override
    public Site nodeFactory(final String id) {
	return new Site(id);
    }

    @Override
    public RadioLink edgeFactory(final String id, final String sourceID,
	    final String destID) {
	return new RadioLink(id, sourceID, destID);
    }

    /*
     * Used by dijkstra's shortest path algorithm 
     */
    public String dijkstra_getNextClosestNodeID(
	    final HashSet<String> shortestDistanceNotFound,
	    final HashMap<String, Double> distance) {
	Double nextClosestDist = new Double(Double.POSITIVE_INFINITY);
	String nextClosestNodeID = null;
	final Iterator<String> j = shortestDistanceNotFound.iterator();
	while (j.hasNext()) {
	    final String jid = (String) j.next();
	    if ((distance.get(jid)).compareTo(nextClosestDist) < 0) {
		nextClosestDist = distance.get(jid);
		nextClosestNodeID = jid;
	    }
	}
	return nextClosestNodeID;
    }

    /**
     * Compute the shortest distance between two vertices using Dijkstra's algorithm
     * @param 	sourceID	The identifier of the source vertex.
     * @param 	destID		The identifier of the destination vertex. 
     * 
     */
    public Path getShortestPath(final String sourceID, 
    		final String destID,
    		final LinkCostMetric linkCostMetric) {
    	
	final HashMap<String, Double> distance = new HashMap<String, Double>();
	final HashMap<String, String> previous = new HashMap<String, String>();
	final HashSet<String> shortestDistanceFound = new HashSet<String>();
	final HashSet<String> shortestDistanceNotFound = new HashSet<String>();

	logger.info("Computing shortest path from " + sourceID + " to "
		+ destID + " using cost metric " + linkCostMetric);
	final Iterator<String> i = this.nodes.keySet().iterator();
	while (i.hasNext()) {
	    final String vid = i.next();
	    distance.put(vid, new Double(Double.POSITIVE_INFINITY));
	    shortestDistanceNotFound.add(vid);
	}
	distance.put(sourceID, new Double(0));

	//find the next closest node
	while (!shortestDistanceNotFound.isEmpty()) {

	    final String nextClosestVertexID = this
		    .dijkstra_getNextClosestNodeID(shortestDistanceNotFound,
			    distance);
	    if (nextClosestVertexID == null) {
		break;
	    }

	    shortestDistanceNotFound.remove(nextClosestVertexID);
	    shortestDistanceFound.add(nextClosestVertexID);

	    final Node[] nextClosestNodes = this.nodes.get(nextClosestVertexID)
		    .getOutputs();
	    for (Node element : nextClosestNodes) {
		final Site n = (Site) element;
		final RadioLink e = (RadioLink) this.edges.get(this
			.generateEdgeID(nextClosestVertexID, n.getID()));
    if(e != null)
    {
		final double try_d = (distance.get(nextClosestVertexID))
			.doubleValue()
			+ e.getCost(linkCostMetric);
		final double current_d = (distance.get(n.getID()))
			.doubleValue();
		if (try_d < current_d) {
		    distance.put(n.getID(), new Double(try_d));
		    previous.put(n.getID(), nextClosestVertexID); //?
		}
		}
	    }
	    
	}

	Path path = new Path((Site) this.nodes.get(destID));
	
	if (previous.containsKey(destID)) {
	    String siteID = destID;
	    while (previous.get(siteID) != null) {
	    	siteID = previous.get(siteID);
	    	path.prepend(this.getSite(siteID));
	    }
	    logger.info("Shortest path: " + path.toString());
	} else if (!sourceID.equals(destID)) {
	    logger.info("Sites " + sourceID + " and " + destID
		    + " are not linked.");
	    path = null;
	}
	return path;
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
//    public RT steinerTree(final int sink, final int[] sources,
//	    final String name, int randomSeed) {
//	final RT steinerTree = new RT(name, this.getSite(sink));
//
//	final ArrayList<String> nodesToAdd = new ArrayList<String>();
//	final ArrayList<String> nodesAdded = new ArrayList<String>();
//
//	for (int element : sources) {
//	    nodesToAdd.add(new Integer(element).toString());
//	}
//
//	boolean first = true;
//	final Random random = new Random(randomSeed);
//
//	//TODO: bug remains, sink of first routing tree never gets set as source...
//    Iterator<Node> siteIter = nodes.values().iterator();
//    while (siteIter.hasNext()) {
//    	Site s = (Site) siteIter.next();
//    	if (nodesToAdd.contains(s.getID())) {
//    		s.setIsSource(true);
//    	}
//    }
//	
//	while (nodesToAdd.size() > 0) {
//
//	    //get a source node at random
//	    final int randomPos = random.nextInt(nodesToAdd.size());
//	    final String sourceNodeID = nodesToAdd.get(randomPos);
//	    nodesToAdd.remove(randomPos);
//
//	    LinkCostMetric linkCostMetric;
//
//	    //TODO: This needs to be sorted out
//	    linkCostMetric = LinkCostMetric.RADIO_LOSS;
//	    
//	    Path shortestPath;
//	    if (first) {
//			//the first time, get the path from the currentSource to the sink
//			shortestPath = this.getShortestPath(sourceNodeID, new Integer(
//				sink).toString(), linkCostMetric);
//			nodesAdded.add(new Integer(sink).toString());
//			first = false;
//
//	    } else {
//			//add path from source node to a random destination node added to the steiner tree 
//			final String destNode = nodesAdded.get(random
//				.nextInt(nodesAdded.size()));
//			shortestPath = this.getShortestPath(sourceNodeID, destNode, linkCostMetric);
//	    }
//	    
//	    //now traverse shortest path from the currentSource, adding edges to the steiner tree,
//	    //until you find a node already in the steiner tree
//	    String tmpPrev = sourceNodeID;
//	    String tmpCurrent = sourceNodeID;
//	    boolean foundFlag = false;
//
//	    if (!steinerTree.nodes.containsKey(sourceNodeID)) {
//	    	Iterator<Site> pathIter = shortestPath.iterator();
//	    	while (pathIter.hasNext() && !foundFlag) {
//			    tmpPrev = tmpCurrent;
//			    tmpCurrent = pathIter.next().getID();
//	
//			    if (steinerTree.nodes.containsKey(tmpCurrent)) {
//			    	foundFlag = true;
//			    }
//	
//			    if (!tmpPrev.equals(tmpCurrent)) {
//			    	String linkID = this.generateEdgeID(tmpPrev, tmpCurrent);
//			    	RadioLink oldLink = (RadioLink) this.getEdge(linkID);
//			    	steinerTree.addExternalSiteAndRadioLinkClone(
//			    			this.getSite(tmpPrev), this.getSite(tmpCurrent), oldLink);
//			    }	
//	
//			    logger.info("Added edge to steiner tree:" + tmpPrev + "-"
//				    + tmpCurrent);
//	    	}
//		}
//
//		nodesAdded.add(sourceNodeID.toString());
//	    
//	}
//	return steinerTree;
//    }

//    /**
//     * Exports the graph as a file in the DOT language used by GraphViz,
//     * @see http://www.graphviz.org/
//     * 
//     * @param fname 	the name of the output file
//     */
//    @Override
//    public void exportAsDOTFile(final String fname, final String label) {
//	try {
//	    final PrintWriter out = new PrintWriter(new BufferedWriter(
//		    new FileWriter(fname)));
//
//	    String edgeSymbol;
//	    if (this instanceof RT) {
//		out.println("digraph \"" + this.getName() + "\" {");
//		edgeSymbol = "->";
//	    } else {
//		out.println("graph \"" + this.getName() + "\" {");
//		edgeSymbol = "--";
//	    }
//
//	    out.println("size = \"8.5,11\";"); // do not exceed size A4
//	    out.println("label = \"" + label + "\";");
//	    out.println("rankdir=\"BT\";");
//
//	    final Iterator j = this.nodes.keySet().iterator();
//	    while (j.hasNext()) {
//			final Site n = (Site) this.nodes.get(j.next());
//			out.print(n.getID() + " [fontsize=20 ");
//			
//			if (n.isSource()) {
//				out.print("shape = doublecircle ");
//			}
//				
//			out.print("label = \"" + n.getID());
//			if (Settings.DISPLAY_SITE_PROPERTIES) {
//				out.print("\\nenergy stock: " + n.getEnergyStock() + "\\n");
//				out.print("RAM: " + n.getRAM() + "\\n");
//				out.print(n.getFragmentsString() + "\\n");
//				out.print(n.getExchangeComponentsString());
//			}
//			
//			out.println("\"];");
//	    }
//
//	    //traverse the edges now
//	    final Iterator i = this.edges.keySet().iterator();
//	    while (i.hasNext()) {
//		final RadioLink e = (RadioLink) this.edges.get(i.next());
//		
//		//This prevents bi-directional links from being drawn twice and thus cluttering up
//		//the topology graph
//		if ((Integer.parseInt(e.getSourceID()) < Integer.parseInt(e.getDestID())) || (this instanceof RT)) {
//			out.print("\"" + this.nodes.get(e.getSourceID()).getID() + "\""
//					+ edgeSymbol + "\""
//					+ this.nodes.get(e.getDestID()).getID() + "\" ");
//
//				if (Settings.DISPLAY_SENSORNET_LINK_PROPERTIES) {
//				    //TODO: find a more elegant way of rounding a double to 2 decimal places
//				    out.print("[label = \"radio loss =" 
//				    		+ (Math.round(e.getRadioLossCost() * 100))
//				    		/ 100 + "\\n ");
//				    out.print("energy = " + e.getEnergyCost() + "\\n");
//				    out.print("latency = " + e.getLatencyCost() + "\"");
//				} else {
//				    out.print("[");
//				}
//
//				out.println("style = dashed]; ");
//
//			    }
//		}
//
//	    out.println("}");
//	    out.close();
//
//	} catch (final IOException e) {
//		//TODO: do something!
//	}
//    }

    
//    protected void cloneNodesAndEdges(Topology clone) {
//    	// create shallow clones of each node in nodes collection
//    	// don't link any nodes yet
//    	Iterator<String> siteIDIter = this.nodes.keySet().iterator();
//    	while (siteIDIter.hasNext()) {
//    	    final String siteID = siteIDIter.next();
//    	    final Site clonedSite = ((Site) this.nodes.get(siteID))
//    		    .shallowClone();
//    	    clone.addNode(clonedSite);
//    	}
//
//    	// now link all the nodes
//    	// this relies on the id of nodes of the clone being the same as the sensor network graph
//    	siteIDIter = clone.nodes.keySet().iterator();
//    	while (siteIDIter.hasNext()) {
//    	    final String siteID = siteIDIter.next();
//    	    final Site site = (Site) this.nodes.get(siteID);
//    	    final Site siteClone = (Site) clone.nodes.get(siteID);
//
//    	    for (int i = 0; i < site.getInDegree(); i++) {
//	    		final String childID = site.getInput(i).getID();
//	    		final Site childClone = (Site) clone.nodes.get(childID);
//	    		siteClone.setInput(childClone, i);
//    	    }
//
//    	    for (int i = 0; i < site.getOutDegree(); i++) {
//	    		final String parentID = site.getOutput(i).getID();
//	    		final Site parentClone = (Site) clone.nodes.get(parentID);
//	    		siteClone.setOutput(parentClone, i);
//    	    }
//    	}
//
//    	// alternatively update edges could be used...
//    	clone.edges = (TreeMap<String, Edge>) this.edges.clone();
//    }
    
//    /**
//     * Clones an existing graph.  The nodes and edges of the graph are also cloned.
//     */
//    @Override
//    public Topology clone() {
//    	final Topology clone = new Topology(this.name + "clone", this.directed);
//		this.cloneNodesAndEdges(clone);
//		return clone;
//    }
    
    public RadioLink getRadioLink(final Site source, final Site dest) {
    	String id = this.generateEdgeID(source.getID(), dest.getID());
    	RadioLink link = (RadioLink) this.getEdge(id);
    	return link;
    }
    
    public double getRadioLossCost(final Site source, final Site dest) {
    	RadioLink link = this.getRadioLink(source, dest);
    	return link.getRadioLossCost();
    }    
    
    public double getLinkLatencyCost(final Site source, final Site dest) {
    	RadioLink link = this.getRadioLink(source, dest);
    	return link.getLatencyCost();
    }    
    
    public double getLinkEnergyCost(final Site source, final Site dest) {
    	RadioLink link = this.getRadioLink(source, dest);
    	return link.getEnergyCost();
    }

	public Iterator<Node> siteIterator() {
		return this.nodes.values().iterator();
	}

  public void removeNodeAndAssociatedEdges(String nodeID)
  {
    ArrayList<Edge> edgesSet =  new ArrayList<Edge>(edges.values());
    Iterator<Edge> edgeIterator = edgesSet.iterator();
    while(edgeIterator.hasNext())
    {
      Edge edge = edgeIterator.next();
      if(edge.getDestID().equals(nodeID) || edge.getSourceID().equals(nodeID))
        this.removeEdge(this.getSite(edge.getSourceID()), this.getSite(edge.getDestID()));
    }
    this.removeNode(nodeID);
  }
  
  public void removeAssociatedEdges(String nodeID)
  {
    ArrayList<Edge> edgesSet =  new ArrayList<Edge>(edges.values());
    Iterator<Edge> edgeIterator = edgesSet.iterator();
    while(edgeIterator.hasNext())
    {
      Edge edge = edgeIterator.next();
      if(edge.getDestID().equals(nodeID) || edge.getSourceID().equals(nodeID))
        this.removeEdge(this.getSite(edge.getSourceID()), this.getSite(edge.getDestID()));
    }
  }


    
    /**
     * TODO: make this a unit test
     * 
    public static void main(final String[] args) {

	final Options opt = new Options(args, Options.Multiplicity.ZERO_OR_ONE);
	Settings.initialize(opt);

	//Graph 1
	final Topology ug = new Topology("test", true);
	TopologyReader.readTopologyNSSFile(
			ug, Settings.INPUTS_NETWORK_TOPOLOGY_FILE);
	ug.display(Settings.GENERAL_OUTPUT_ROOT_DIR, ug.getName());

	//Graph 2
	/*Topology ug2 = new Topology("test2",true);
	 TopologyReader.readTopologyNSSFile(ug2,"input/100-node-topology.nss");
	 System.out.println(ug2.getNumNodes());
	 ug2.display("output/output", ug2.getName());
	 
	 //Merge graphs 1 and 2
	 ug.mergeGraphs(ug2);
	 //ug.display();
	 
	//Find the shortest path from dijkstraSource to dijkstraDest
	final Path p = ug.getShortestPath("0", "9", LinkCostMetric.RADIO_LOSS);
	System.out.println(p.toString());

	//Generate Steiner trees (non-determinstic): numResult 
	//possibilities are generated with dest and sources as steiner nodes
	for (int n = 0; n < 3; n++) {
		RT st = ug.steinerTree(3, new int[]{1, 2, 4, 5, 6, 8}, "steiner", 4);
		st.display(Settings.GENERAL_OUTPUT_ROOT_DIR, st.getName());
	}

    } */
}
