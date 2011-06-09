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

/**
 * @author Ixent Galpin
 * 
 * Edge in the Graph ADT.
 *  
 * Inspired by http://www.alexander-merz.com/graphviz/doc/index.html
 * 
 */

public class EdgeImplementation implements Edge, Comparable{

	private String id;

	private String sourceID;

	private String destID;

	public EdgeImplementation(String id, String sourceID, String targetID) {
		this.id = id;
		this.sourceID = sourceID;
		this.destID = targetID;
	}

	public String getSourceID() {
		return sourceID;
	}

	public String getDestID() {
		return destID;
	}

	public String getID() {
		return id;
	}

	public String getOtherVertexID(String v1id) {
		String v2id;
		if (this.sourceID.equals(v1id)) {
			v2id = this.destID;
		} else {
			v2id = this.sourceID;
		}
		return v2id;
	}

	public EdgeImplementation clone() {
		return new EdgeImplementation(this.id, this.sourceID, this.destID);
	}

	/**
	 * The ids must match, as when a graph is cloned the edge ids do not change.
	 */
	public boolean isEquivalentTo(Edge otherEdge) {

		if (!this.getID().equals(otherEdge.getID())) {
			return false;
		}

		if (!this.getSourceID().equals(otherEdge.getSourceID())) {
			return false;
		}

		if (!this.getDestID().equals(otherEdge.getDestID())) {
			return false;
		}

		return true;
	}

  @Override
  public int compareTo(Object o)
  {
    EdgeImplementation other = (EdgeImplementation) o;
    if( this.id.equals((other.id)))
      return 0;
    else
      return -1;
  }

}
