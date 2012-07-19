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

package uk.ac.manchester.cs.snee.common.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;


/**
 * @author Ixent Galpin
 * 
 * A generic node in a generic Graph, providing basic functionality to maintain the structure
 * of the graph, and add, delete, insert, replace nodes and edges.  
 * Domain specific graphs should inherit from this class, and also from the Node and Edge class. 
 *  
 * Inspired by http://www.alexander-merz.com/graphviz/doc/index.html
 * 
 */

public class Graph implements Cloneable, Serializable{

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 5022829555891626720L;

  private static final Logger logger = Logger.getLogger(Graph.class.getName());

	/**
	 * The name of the graph
	 */
	protected String name;

	/**
	 * Specifies whether the graph is directed or not
	 */
	protected boolean directed;

	/**
	 * Specifies whether more than one edge may exist with the same source/dest
	 */
	protected boolean allowsMutipleEdges;

	/** 
	 * The nodes of the graph. 
	 */
	protected TreeMap<String, Node> nodes = new TreeMap<String, Node>();

	public TreeMap<String, Edge> getEdges()
  {
    return edges;
  }

  /**
	 * The edges of the graph.
	 */
	protected TreeMap<String, Edge> edges = new TreeMap<String, Edge>();

	/** 	
	 * Constructor for Graph class.
	 * @param name 		the name of the graph
	 * @param directed 	whether the graph should have directed edges or not
	 * 
	 */
	public Graph(String name, boolean directed, boolean allowsMutipleEdges) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER: " + name + " directed: " + directed +
					" Multiple Edges: " + allowsMutipleEdges);
		this.name = name;
		this.directed = directed;
		this.allowsMutipleEdges = allowsMutipleEdges;
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	/**
	 * Default graph constructor.
	 *
	 */
	public Graph() {
		this("graph", true, false);
	}

	public Graph(String name) {
		this(name, true, false);
	}

	/**
	 * Constructor used for cloning.
	 * @param g The graph to be cloned
	 */
	public Graph(Graph g, String inName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER " + g + " name: " + inName);
		this.name = inName;
		this.directed = g.directed;
		this.allowsMutipleEdges = g.allowsMutipleEdges;    	
		g.copyNodesAndEdges(this);
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	/**
	 * Returns the name of the graph.
	 */
	public String getName() {
		return this.name;
	}
	
	public String toString() {
		return name; 
	}

	/**
	 * Returns whether the graph is directed or not.
	 */
	public boolean isDirected() {
		return this.directed;
	}

	/**
	 * Gets the node with a specific id
	 * @param id	the id of the node
	 * @return		the node in the graph with that id
	 */
	public Node getNode(String id) {
		return nodes.get(id);
	}

	/**
	 * Gets the node with a specific id
	 * @param id	the id of the node
	 * @return		the node in the graph with that id
	 */
	public Node getNode(int id) {
		return nodes.get(new Integer(id).toString());
	}

	/**
	 * Returns a particular edge
	 * @param eid
	 * @return
	 */
	public Edge getEdge(String id) {
		return edges.get(id);
	}

	/**
	 * Returns the number of vertices the graph contains.
	 */
	public int getNumNodes() {
		return this.nodes.size();
	}

	/**
	 * Returns the number of edges the graph contains.
	 */
	public int getNumEdges() {
		return edges.size();
	}

	public HashSet<EdgeImplementation> getNodeEdges(String nodeID) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER " + nodeID);
		HashSet<EdgeImplementation> results = new HashSet<EdgeImplementation>();

		Iterator<Edge> edgeIter = this.edges.values().iterator();
		while (edgeIter.hasNext()) {
			Edge e = edgeIter.next();
			if (e.getSourceID().equals(nodeID) || e.getDestID().equals(nodeID)) {
				results.add((EdgeImplementation) e);
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN: " + results);
		return results;
	}

	/**
	 * Adds a disconnected node to the graph.  
	 * If node with same id already in the graph, returns it
	 * 
	 * @param newNode	adds a newNode to the graph
	 * 
	 */
	public Node addNode(Node newNode) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER " + newNode);
		Node node;
		if (!nodes.containsKey(newNode.getID())) {
			nodes.put(newNode.getID(), newNode);
			node = newNode;
		} else
			node = nodes.get(newNode.getID());
		if (logger.isDebugEnabled())
			logger.debug("RETURN " + node);
		return node;
	}

	public NodeImplementation nodeFactory(String id) {
		return new NodeImplementation(id);
	}

	//used for cloning purposes
	public NodeImplementation nodeFactory(NodeImplementation node) {
		return new NodeImplementation(node.getID());
	}

	public EdgeImplementation edgeFactory(String id, String sourceID,
			String destID) {
		return new EdgeImplementation(id, sourceID, destID);
	}

	//used for cloning purposes
	public EdgeImplementation edgeFactory(EdgeImplementation edge) {
		return new EdgeImplementation(edge.getID(), edge.getSourceID(), edge.getDestID());
	}

	/**
	 * Adds a disconnected node to the graph.  
	 * If already in the graph, does nothing.
	 * 
	 * @param id 	the identifier of the node.
	 * 
	 */
	public Node addNode(String id) {

		if (!nodes.containsKey(id)) {
			Node v = nodeFactory(id);
			nodes.put(id, v);
			return v;
		} else
			return null;
	}

	public String generateEdgeID(String source, String dest) {
		if (this.directed) {
			if (this.allowsMutipleEdges) {
				int n;
				for (n = 0; this.edges.containsKey(source + "->" + dest + ":"
						+ n); n++) {
				}
				return source + "->" + dest + ":" + n;
			} else
				return source + "->" + dest;
		} else {
			String str;
			if (source.compareTo(dest) < 0)
				str = (source + "--" + dest);
			else
				str = (dest + "--" + source);

			if (this.allowsMutipleEdges) {
				int n;
				for (n = 0; this.edges.containsKey(str + ":" + n); n++) {
				}
				return str + ":" + n;
			} else {
				return str;
			}
		}
	}

	/**
	 * 
	 * Add an edge between two nodes. 
	 * @param source	The source  vertex.
	 * @param dest	The destination vertex.
	 * @param bidirectional
	 * If either of the nodes do not exist in the graph, they are added.
	 */
	public EdgeImplementation addEdge(Node source, Node dest,
			boolean bidirectional) {
		// add nodes if not in graph
		if (!nodes.containsKey(source.getID()))
			nodes.put(source.getID(), source);
		if (!nodes.containsKey(dest.getID()))
			nodes.put(dest.getID(), dest);

		String eid = generateEdgeID(source.getID(), dest.getID());

		//link the nodes with each other, and update edges
		if (edges.containsKey(eid)) {
			//TODO: We only allow one edge between any two vertices
			//However nss file needs both ways.
			logger.trace("Duplicate edge, ignored.");
		} else if (!source.hasOutput(dest)) {
			source.addOutput(dest);
			dest.addInput(source);
		}

		//	update the edges collection
		if (!edges.containsKey(eid)) {
			EdgeImplementation e = edgeFactory(eid, source.getID(), dest
					.getID());
			edges.put(eid, e);
			return e;
		}

		if (bidirectional || !this.isDirected()) {
			eid = generateEdgeID(dest.getID(), source.getID());

			if (edges.containsKey(eid)) {
				//TODO: We only allow one edge between any two vertices
				//However nss file needs both ways.
				logger.trace("Duplicate edge, ignored.");
			} else if (!source.hasOutput(dest)) {
				dest.addOutput(source);
				source.addInput(dest);
			}

			if (!edges.containsKey(eid)) {
				EdgeImplementation e = edgeFactory(eid, dest.getID(), source
						.getID());
				edges.put(eid, e);
				return e;
			}
		}

		return (EdgeImplementation) edges.get(eid);
	}

	/**
	 * Add an edge between two nodes
	 * @param source	the source node
	 * @param dest	the destination node
	 * If either of the nodes do not exist in the graph, they are added.
	 */
	public EdgeImplementation addEdge(Node source, Node dest) {
		return addEdge(source, dest, true);
	}

	/**
	 * Add an edge between two nodes. 
	 * @param sourceID	The id of the source node
	 * @param destID	The id of the destination node.
	 * If either of the nodes do not exist in the graph, they are created.
	 * 
	 */
	public EdgeImplementation addEdge(String sourceID, String destID,
			boolean bidirectional) {
		Node source;
		Node dest;

		//create the source node if it doesn't already exist
		if (nodes.containsKey(sourceID)) {
			source = nodes.get(sourceID);
		} else {
			source = addNode(sourceID);
		}

		//create the dest node if it doesn't already exist
		if (nodes.containsKey(destID)) {
			dest = nodes.get(destID);
		} else {
			dest = addNode(destID);
		}
		return addEdge(source, dest, bidirectional);
	}

	public Collection<Node> getNodes() {
		return nodes.values();
	}
	
	 public  TreeMap<String, Node> getAllNodes() {
	    return nodes;
	  }

	/**
	 * Removes node from the graph
	 * @param id	id of the node to be removed
	 */
	public void removeNode(String id) {
		//remove the node from the nodes collection
		nodes.remove(id);
		logger.trace("removing node:" + id);
		//remove any edges which reference the node
		//Iterator<String> edgeIter = edges.keySet().iterator();
		Iterator<Entry<String, Edge>> edgeIter = edges.entrySet().iterator();
		while (edgeIter.hasNext()) {
			Entry<String, Edge> eid = edgeIter.next();
			Edge e = eid.getValue();
			if (e.getSourceID().equals(id) || e.getDestID().equals(id)) {
				edgeIter.remove();
				logger.trace("removing edge");
			}
		}
	}
	
	/**
	 * removes a node from the graph, takes out all links to the node as well.
	 * @param remove
	 */
	public void removeNodeWithoutLinkage(Node remove) 
	{
    //remove the node from the nodes collection
    nodes.remove(remove.getID());
    logger.trace("removing node:" + remove.getID());
    //remove any edges which reference the node
    //Iterator<String> edgeIter = edges.keySet().iterator();
    Iterator<Entry<String, Edge>> edgeIter = edges.entrySet().iterator();
    while (edgeIter.hasNext()) 
    {
      Entry<String, Edge> eid = edgeIter.next();
      Edge e = eid.getValue();
      if (e.getSourceID().equals(remove.getID()) || e.getDestID().equals(remove.getID())) 
      {
        edgeIter.remove();
        logger.trace("removing edge");
      }
    }
  }

	/**
	 * Removes node from the graph
	 * Links the output and the input up so can only be called on nodes 
	 * that have exactly one input and one output.
	 *  
	 * @param node	node to be removed
	 */
	public void removeNode(Node remove) throws OptimizationException {
		Node[] inputs = remove.getInputs();
		if (inputs.length != 1)
			throw new OptimizationException("Unable to remove node " + remove
					+ " as it does not have exactly one input");
		Node[] outputs = remove.getOutputs();
		if (outputs.length != 1)
			throw new OptimizationException("Unable to remove node " + remove
					+ " as it does not have exactly one output");
		inputs[0].replaceOutput(remove, outputs[0]);
		outputs[0].replaceInput(remove, inputs[0]);
		addEdge(inputs[0], outputs[0]);

		removeNode(remove.getID());
	}

     /**
	 * Removes an edge from the graph
	 * @param source	The source node of the edge
	 * @param dest	The destination node of the edge
	 */
	public void removeEdge(Node source, Node dest) {
		String eid = generateEdgeID(source.getID(), dest.getID());

		if (edges.containsKey(eid)) {
			source.removeOutput(dest);
			dest.removeInput(source);
			edges.remove(eid);
		}
	}

	/**
	 * Inserts a new node between two nodes source and dest
	 * @param source
	 * @param dest
	 * @param n
	 */
	public void insertNode(Node source, Node dest, Node newNode) {
		String eid = generateEdgeID(source.getID(), dest.getID());

		if (edges.containsKey(eid)) {
			addNode(newNode);
			source.replaceOutput(dest, newNode);
			dest.replaceInput(source, newNode);
			newNode.addInput(source);
			newNode.addOutput(dest);
			removeEdge(source, dest);
			addEdge(source, newNode);
			addEdge(newNode, dest);
		} else {
			logger.fatal("edges not found");
		}
	}

	/**
	 * Inserts a path between two nodes.  The path is inserted between the source and 
	 * destination nodes, using the same ports as are linking the source and dest prior to 
	 * the insertion. 
	 * @param source	source node
	 * @param dest	source node
	 * @param path		ordered array of nodes to be inserted
	 */
	public void insertPath(Node source, Node dest, Node[] path) {

		Node prev = source;
		for (int i = 0; i < path.length; i++) {
			insertNode(prev, dest, path[i]);
			prev = path[i];
		}
	}

	//CD new method
	/**
	 * Replaces a node in the graph with a new version of the same node.  
	 * The edges are unchanged as one node is assumed to replace the other.
	 * @param replace	node in graph to be replaced
	 * @param newNode	replacement node
	 */
	private void replaceSameNode(Node replace, Node newNode) {
		logger.trace("start Size = " + getNumNodes());
		logger.trace(replace.getID() + " and " + newNode.getID());
		nodes.put(newNode.getID(), newNode);
		Node[] inputs = replace.getInputs();
		for (int i = 0; i < inputs.length; i++) {
			Node n = inputs[i];
			n.replaceOutput(replace, newNode);
			newNode.addInput(n);
		}
		logger.trace("Mid Size = " + getNumNodes());

		Node[] outputs = replace.getOutputs();
		for (int i = 0; i < outputs.length; i++) {
			Node n = outputs[i];
			n.replaceInput(replace, newNode);
			newNode.addOutput(n);
		}
		logger.trace("Last Size = " + getNumNodes());
	}

	//CB OLD method
	/**
	 * Replaces a node in the graph with a new node.  The edges of the old node are linked to
	 * the same ports of the new node.
	 * @param replace	node in graph to be replaced
	 * @param newNode	replacement node
	 */
	private void replaceDifferentNodes(Node replace, Node newNode) {
		Node[] inputs = replace.getInputs();
		for (int i = 0; i < inputs.length; i++) {
			Node n = inputs[i];
			n.replaceOutput(replace, newNode);
			newNode.addInput(n);
			removeEdge(n, replace);
			addEdge(n, newNode);
		}
		Node[] outputs = replace.getOutputs();
		for (int i = 0; i < outputs.length; i++) {
			Node n = outputs[i];
			n.replaceInput(replace, newNode);
			newNode.addOutput(n);
			removeEdge(replace, n);
			addEdge(newNode, n);
		}
		nodes.remove(replace.getID());
	}

	//CB TEMP FIX	
	public void replaceNode(Node replace, Node newNode) {
		if (replace.getID() == newNode.getID())
			replaceSameNode(replace, newNode);
		else
			replaceDifferentNodes(replace, newNode);
	}

	/**
	 * Replaces a node in the graph with a path.
	 * The first node in the path is linked with the original nodes outputs.
	 * The last node in the path is linked with the original nodes inputs.
	 * @param replace	the node to be replaced
	 * @param path		an ordered array of nodes specifying the path to be inserted
	 */
	public void replacePath(Node replace, Node[] path) {

		Node prev = null;
		for (int i = 0; i < path.length; i++) {

			Node newNode = path[i];

			//first node in path
			if (i == 0) {
				Node[] outputs = replace.getOutputs();
				for (int j = 0; j < outputs.length; j++) {
					Node n = outputs[j];
					n.replaceInput(replace, newNode);
					newNode.addOutput(n);
					removeEdge(replace, n);
					addEdge(newNode, n);
				}
				prev = path[0];
			} else {
				//node after the first
				prev.addInput(newNode);
				newNode.addOutput(prev);
				addEdge(newNode, prev);
			}

			//	last node in path
			if (i == path.length - 1) {
				Node[] inputs = replace.getInputs();
				for (int j = 0; j < inputs.length; j++) {
					Node n = inputs[j];
					n.replaceOutput(replace, newNode);
					newNode.addInput(n);
					removeEdge(n, replace);
					addEdge(n, newNode);
				}
			}
		}
		nodes.remove(replace.getID());
	}

	/**
	 * Merges another graph with the current one.
	 * @param otherGraph the graph to be incorporated with this one.
	 */
	public void mergeGraphs(Graph otherGraph) {

		Iterator<String> i = otherGraph.edges.keySet().iterator();
		while (i.hasNext()) {
			Edge otherEdge = (Edge) otherGraph.edges.get(i.next());
			if (!this.edges.containsKey(otherEdge.getID())) {
				Node source, dest;

				if (!this.nodes.containsKey(otherEdge.getSourceID())) {
					source = this.addNode(otherGraph.nodes.get(
							otherEdge.getSourceID()).shallowClone());
				} else {
					source = this.nodes.get(otherEdge.getSourceID());
				}

				if (!this.nodes.containsKey(otherEdge.getDestID())) {
					dest = this.addNode(otherGraph.nodes.get(
							otherEdge.getDestID()).shallowClone());
				} else {
					dest = this.nodes.get(otherEdge.getDestID());
				}

				edges.put(otherEdge.getID(), otherEdge.clone());
				source.addOutput(dest);
				dest.addInput(source);
			}
		}

	}
	
	/**
   * Merges another graph with the current one.
   * @param otherGraph the graph to be incorporated with this one.
	 * @param network 
   */
  public void mergeGraphsUsingSites(Graph otherGraph, Topology network) {

    Iterator<String> i = otherGraph.edges.keySet().iterator();
    while (i.hasNext()) {
      Edge otherEdge = (Edge) otherGraph.edges.get(i.next());
      if (!this.edges.containsKey(otherEdge.getID())) {
        Site sourceSite, destSite; 

        if (!this.nodes.containsKey(otherEdge.getSourceID())) 
        {
          sourceSite = new Site((Site)otherGraph.nodes.get(otherEdge.getSourceID()));
          sourceSite = (Site) this.addNode(sourceSite);
        } 
        else 
        {
          sourceSite = (Site) this.nodes.get(otherEdge.getSourceID());
        }

        if (!this.nodes.containsKey(otherEdge.getDestID())) 
        {
          destSite = new Site((Site)otherGraph.nodes.get(otherEdge.getDestID()));
          destSite = (Site) this.addNode(destSite);
        } 
        else 
        {
          destSite = (Site) this.nodes.get(otherEdge.getDestID());
        }
        RadioLink otherRadioLinkForwards = network.getRadioLink(sourceSite, destSite);
        RadioLink otherRadioLinkBackwards = network.getRadioLink(destSite, sourceSite);
        
        otherRadioLinkForwards = new RadioLink(otherRadioLinkForwards);
        otherRadioLinkBackwards = new RadioLink(otherRadioLinkBackwards);
        edges.put(otherRadioLinkForwards.getID(), otherRadioLinkForwards);
        edges.put(otherRadioLinkBackwards.getID(), otherRadioLinkBackwards);
        sourceSite.addOutput(destSite);
        sourceSite.addInput(destSite);
        destSite.addInput(sourceSite);
        destSite.addOutput(sourceSite);
      }
    }

  }
	

	/**
	 * Exports the graph as a file in the DOT language used by GraphViz.
	 * @see http://www.graphviz.org/
	 * 
	 * @param fname 	the name of the output file
	 * @throws SchemaMetadataException 
	 */
	public void exportAsDOTFile(String fname, String label) 
	throws SchemaMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER exportAsDOTFile() with " + fname + " " +
					label);
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(fname)));

			String edgeSymbol;
			if (this.isDirected()) {
				out.println("digraph \"" + (String) this.getName() + "\" {");
				edgeSymbol = "->";
			} else {
				out.println("graph \"" + (String) this.getName() + "\" {");
				edgeSymbol = "--";
			}

			out.println("label = \"" + label + "\";");
			out.println("rankdir=\"BT\";");

			//traverse the edges now
			Iterator<String> i = edges.keySet().iterator();
			while (i.hasNext()) {
				Edge e = edges.get((String) i.next());
				out.println("\"" + this.nodes.get(e.getSourceID()).getID()
						+ "\"" + edgeSymbol + "\""
						+ this.nodes.get(e.getDestID()).getID() + "\" ");
			}
			out.println("}");
			out.close();
		} catch (IOException e) {
			logger.warn("Unable to write to file " + fname);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	public void exportAsDOTFile(String fname) 
	throws SchemaMetadataException {
		exportAsDOTFile(fname, "");
	}
	
	/**
	 * Converts a file in the DOT, the Graphviz graph specification 
	 * language into a PNG image.
	 * @param inputFullPath	the DOT file
	 * @param outputFullPath the PNG image
	 */
	public void convertDOT2PNG(String inputFullPath, String outputFullPath) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER convertDOT2PNG() with " + inputFullPath + 
					" " + outputFullPath);
		//Code adapted from	
		//http://www.javaworld.com/javaworld/jw-12-2000/jw-1229-traps.html?page=3
		try {
			Runtime rt = Runtime.getRuntime();
			if (logger.isTraceEnabled()) {
				logger.trace(
						SNEEProperties.getSetting(SNEEPropertyNames.GRAPHVIZ_EXE) +
						"-Tpng " + 
						"-o" + outputFullPath + " " + inputFullPath);
			}
			Process proc = rt.exec(new String[] {
					SNEEProperties.getSetting("graphviz.exe"),
					"-Tpng", "-o" + outputFullPath, inputFullPath });
			InputStream stderr = proc.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			BufferedReader br = new BufferedReader(isr);
			@SuppressWarnings("unused")
      String line = null;
			while ((line = br.readLine()) != null) 
			{}
			int exitVal = proc.waitFor();
			if (logger.isTraceEnabled())
				logger.trace("Dotfile to PNG process exitValue: " + 
						exitVal);
		} catch (Throwable t) {
			String msg = "Dotfile to PNG process failed. " + t;
			logger.warn(msg);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}
	
	/**
	 * Clones an existing graph.  The nodes and edges of the graph are also cloned.
	 */
	public Graph clone() {
		Graph clone = new Graph(this.name + "-clone", this.directed,
				this.allowsMutipleEdges);

		copyNodesAndEdges(clone);

		return clone;
	}

	@SuppressWarnings("unchecked")
  private void copyNodesAndEdges(Graph clone) {
		// create shallow clones of each node in the graph, and add them to the nodes collection
		Iterator<String> nodeIDIter = this.nodes.keySet().iterator();
		while (nodeIDIter.hasNext()) {
			String nodeID = nodeIDIter.next();
			Node clonedNode = this.nodes.get(nodeID).shallowClone();
			clone.addNode(clonedNode);
		}

		// now replace the Node inputs and outputs with references to the clones
		// this effectively results in a deep copy of the nodes collection
		Iterator<Node> nodeIter = this.nodes.values().iterator();
		while (nodeIter.hasNext()) {
			Node n = nodeIter.next();
			Node currentClonedNode = clone.nodes.get(n.getID());

			for (int i = 0; i < n.getInDegree(); i++) {
				String inputNodeID = n.getInput(i).getID();
				Node clonedInputNode = clone.nodes.get(inputNodeID);
				currentClonedNode.setInput(clonedInputNode, i);
				clonedInputNode.addOutput(currentClonedNode);
			}
		}

		clone.edges = (TreeMap<String, Edge>) this.edges.clone();
	}

	/**
	 * Checks whether two graphs have the nodes and edges which are equivalent
	 * (This is defined in turn by the methods in the respective classes).
	 * The names of the graphs do not have to match, as when a graph is cloned
	 * it will most likely be given a different name.
	 * @param otherGraph the other graph to compare
	 * @return whether the two graphs are equivalent
	 */
	public boolean isEquivalentTo(Object other) {

		if (!this.getClass().equals(other.getClass())) {
			return false;
		}
		Graph otherGraph = (Graph) other;

		// check that both graphs have the same number of nodes
		if (nodes.keySet().size() != otherGraph.nodes.keySet().size()) {
			return false;
		}

		// check all nodes in this graph are present in other graph
		Iterator<String> nodeIDIter = nodes.keySet().iterator();
		while (nodeIDIter.hasNext()) {
			String nodeID = nodeIDIter.next();
			Node thisNode = this.getNode(nodeID);
			Node otherNode = otherGraph.getNode(nodeID);
			if (otherNode == null) {
				return false;
			} else if (!thisNode.getClass().equals(otherNode.getClass())) {
				return false;
			} else if (!thisNode.isEquivalentTo(otherNode)) {
				return false;
			}
		}

		// check that both graphs have the same number of edges
		if (edges.keySet().size() != otherGraph.edges.keySet().size()) {
			return false;
		}

		// check all edges in the graph are present in the other graph
		Iterator<String> edgeIDIter = edges.keySet().iterator();
		while (edgeIDIter.hasNext()) {
			String edgeID = edgeIDIter.next();
			Edge thisEdge = this.edges.get(edgeID);
			Edge otherEdge = otherGraph.edges.get(edgeID);
			if (otherGraph.getEdge(edgeID) == null) {
				return false;
			} else if (!thisEdge.getClass().equals(otherEdge.getClass())) {
				return false;
			} else if (!thisEdge.isEquivalentTo(otherEdge)) {
				return false;
			}
		}
		return true;
	}
//
//	//testing script only
//	public static void main(String[] args) {
//
//		Settings.initialize(new Options(args));
//
//		Graph g = new Graph("test", true, false);
//		g.addEdge("1", "2", true);
//		g.addEdge("1", "3", true);
//		g.addEdge("2", "3", true);
//
//		Graph gclone = g.clone();
//
//		System.out.println("Graph g and gclone are equal: " + g.isEquivalentTo(gclone));
//
//		gclone.addEdge("3", "4", true);
//		gclone.addEdge("4", "1", true);
//
//		System.out.println("Graph g and gclone are equal: " + g.isEquivalentTo(gclone));
//
//		Graph h = new Graph("test2", true, false);
//		h.addEdge("1", "2", true);
//		h.addEdge("1", "3", true);
//		h.addEdge("2", "3", true);
//
//		System.out.println("Graph g and h are equal: " + g.isEquivalentTo(h));
//
//		g.display(QueryCompiler.queryPlanOutputDir, g.getName() + "-config");
//		gclone.display(QueryCompiler.queryPlanOutputDir, g.getName()
//				+ "-config");
//	}

}
