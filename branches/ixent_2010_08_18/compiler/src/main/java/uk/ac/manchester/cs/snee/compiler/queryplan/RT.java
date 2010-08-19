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
package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Topology;

/**
 * Class to represent a Routing Tree, data structure used to determine the 
 * data flows in the sensor network.
 *
 */
public class RT extends SNEEAlgebraicForm {

	   
    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(
    		RT.class.getName());    
    
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
	
	Tree routingTree;
	     
	PAF paf;
	
    /**
     * Constructor for the RoutingTree class.
     * @param queryName the name of the routing tree
     * @param inRootSite the sink node for the query
     */
	public RT(PAF paf, final String queryName, final Tree rt) {
		super(queryName);
		this.routingTree = rt;
	}
    
    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
	
	/**
	 * Generates a systematic name for this query plan strucuture, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
    protected String generateName(final String queryName) {
    	candidateCount++;
    	return queryName + "-RT-" + candidateCount;
    }
	
    /**
     * Gets the root site of the routing tree (i.e., the sink for the query). 
     * @return the root site/sink
     */
    public final Site getRoot() {
    	return (Site) this.routingTree.getRoot();
    }
    
    public final Site getSite(String id) {
    	return (Site) this.routingTree.getNode(id);
    }

    /**
     * Given a source site, and a destination site downstream (i.e., further up
     * the tree), returns the path from the source site to the destination site.
     * @param sourceID the id for the source site
     * @param destID the id for the destination site
     * @return the path from the source to destination site
     */
    public final Path getPath(final String sourceID, final String destID) {
    	Path path = new Path();
    	Site source = this.getSite(sourceID);
    	Site current = source;
    	while (current != null) {
    		path.append(current);
    		if (current.getID().equals(destID)) {
    			break;
    		}
    		current = (Site) current.getOutput(0);
    	}
    	
    	// a path was not found from the source and dest
    	if (path.getLastSite() == null) {
    		return new Path();
    	}
    	if (!path.getLastSite().getID().equals(destID)) {
    		return new Path();
    	}
    	
    	return path;
    }
    
    /**
     * Iterator to traverse the routing tree.
     * The structure of the routing tree may not be modified during iteration
     * @param traversalOrder the order to traverse the routing tree
     * @return an iterator for the routing tree
     */    
    public final Iterator<Site> siteIterator(
    		final TraversalOrder traversalOrder) {

    	return this.routingTree.nodeIterator(traversalOrder);
    }

    
	public ArrayList<Site> getSites(ArrayList<String> desiredSites) {
		ArrayList<Site> result = new ArrayList<Site>();
		
		Iterator<String> siteIDIter = desiredSites.iterator();
		while (siteIDIter.hasNext()) {
			result.add(this.getSite(siteIDIter.next()));
		}
		return result;
	}    
    
	public String getProvenanceString() {
		return this.getName()+"-"+this.paf.getProvenanceString();
	}
	
	public PAF getPAF() {
		return this.paf;
	}

	public Tree getSiteTree() {
		return this.routingTree;
	}
}
