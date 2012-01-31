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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

public class NodeImplementation implements Node, Serializable {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2950154515604855704L;

  /**
	 * Identifier of the node
	 */
	protected String id;

	/**
	 * The neighbouring nodes on outgoing edges
	 */
	private List<Node> outputs = new ArrayList<Node>();

	/**
	 * The neighbouring nodes on incoming edges
	 */
	private List<Node> inputs = new ArrayList<Node>();

	/**
	 * Default constructor
	 *
	 */
	public NodeImplementation() {
		super();
	}

	public NodeImplementation(Node model) {
		this.id = model.getID();
	}

	/**
	 * Constructor
	 * @param id	id of the Node
	 */
	protected NodeImplementation(String id) {
		this.id = id;
	}

	/**
	 * Get the id of the node
	 * @return
	 */
	public String getID() {
		return this.id;
	}

	/**
	 * Get the number of incoming edges 
	 * @return
	 */
	public int getInDegree() {
		return this.inputs.size();
	}

	/**
	 * Get the number of outgoing edges
	 * @return
	 */
	public int getOutDegree() {
		return this.outputs.size();
	}

	/**
	 * Get the incoming nodes
	 * @return inouts
	 */
	public List<Node> getInputsList() {
		return inputs;
	}

	/**
	 * Get the incoming nodes
	 * @return
	 */
	//TODO: return ArrayList in future
	public Node[] getInputs() {
		Node[] inputsArray = new Node[inputs.size()];
		for (int i = 0; i < inputs.size(); i++)
			inputsArray[i] = inputs.get(i);
		return inputsArray;
	}

	/**
	 * Returns the input node at the index
	 * specified.
	 */
	public Node getInput(int i) {
		return inputs.get(i);
	}

	/**
	 * Get the outgoing nodes 
	 * @return
	 */
	public Node[] getOutputs() {
		Node[] outputsArray = new Node[outputs.size()];
		for (int i = 0; i < outputs.size(); i++)
			outputsArray[i] = outputs.get(i);
		return outputsArray;
	}

	/**
	 * Returns the output node at the index
	 * specified.
	 */
	public Node getOutput(int i) {
		return outputs.get(i);
	}

	/**
	 * Get the output nodes
	 * @return outouts
	 */
	public List<Node> getOutputsList() {
		return outputs;
	}


	public boolean hasOutput(Node n) {
		return (this.outputs.contains(n));
	}

	public void addInput(Node n) {
		if (!this.inputs.contains(n))
			this.inputs.add(n);
	}

	public void addOutput(Node n) {
		if (!this.outputs.contains(n))
			this.outputs.add(n);
	}

	public void removeOutput(Node target) {
		outputs.remove(target);
	}

	/**
	 * Refreshes the node after something in the structure has changed
	 * Example addition/ change of an input.
	 *
	 */
	protected void refreshNode() {

	}

	public void removeInput(Node source) {
		inputs.remove(source);
	}

	public void setInput(Node n, int index) {
		if (inputs.size() == index) {
			inputs.add(n);
		} else {
			inputs.set(index, n);
		}
		refreshNode();
	}

	public void setOutput(Node n, int index) {
		if (outputs.size() == index) {
			outputs.add(n);
		} else {
			outputs.set(index, n);
		}
	}

	public void replaceInput(Node replace, Node newInput) {
		int i = inputs.indexOf(replace);
		inputs.set(i, newInput);
	}
	
	public void replaceInputByID(Node replace, Node newInput){
	  Iterator<Node> nodeIt = inputs.iterator();
	  while(nodeIt.hasNext())
	  {
	    Node node = nodeIt.next();
	    if(node.getID().equals(replace.getID()))
	    {
	      int i = inputs.indexOf(node);
	      inputs.set(i,newInput);
	    }
	  }
	}

	public void replaceOutput(Node replace, Node newOutput) {
		int i = outputs.indexOf(replace);
		outputs.set(i, newOutput);
	}

	public NodeImplementation shallowClone() {
		NodeImplementation clonedNode = new NodeImplementation(this.id);
		return clonedNode;
	}
	
	public boolean isLeaf() {
		return (this.getInDegree() == 0);
	}

	public boolean isEquivalentTo(Node otherNode) {

		if (!this.getID().equals(otherNode.getID())) {
			return false;
		}

		return true;
	}

  @Override
  public void clearInputs()
  {
    inputs.clear();
  }

  @Override
  public void clearOutputs()
  {
    outputs.clear();  
  }
  
  @Override
  public void removeInput(String nodeid)
  {
    for(int index = 0; index< this.getInDegree(); index ++)
    {
      Node input = this.getInput(index);
      if(input.getID().equals(nodeid))
        this.removeInput(input);
    }
  }
  
  @Override
  public void removeOutput(String nodeid)
  {
    for(int index = 0; index< this.getOutDegree(); index ++)
    {
      Node output = this.getOutput(index);
      if(output.getID().equals(nodeid))
        this.removeOutput(output);
    }
  }

}
