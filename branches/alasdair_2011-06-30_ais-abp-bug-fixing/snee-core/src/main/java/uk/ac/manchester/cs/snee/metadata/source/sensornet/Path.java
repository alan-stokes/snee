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
import java.util.Iterator;

/**
 * Represents a linear path from the network topology.
 * 
 * @author galpini
 */
public class Path {

	/**
	 * List to represent the path of sites.
	 */
	private ArrayList<Site> path = new ArrayList<Site>();
	
	/**
	 * Path constructor.
	 * @param site	The first site to be added to the path
	 */
	public Path(final Site site) {
		path.add(site);
	}

	/**
	 * Creates an empty path.
	 */
	public Path() {
	}

	/**
	 * Add to the beginning of the path.
	 * @param site	Site to be added
	 */
	public final void prepend(final Site site) {
		if (site != null) {
			path.add(0, site);
		}
	}
	
	/**
	 * Add to the end of the path.
	 * @param site  Site to be added
	 */
	public final void append(final Site site) {
		if (site != null) {
			path.add(site);
		}
	}
	
	/**
	 * Iterator to access the sites in order.
	 * @return Iterator to traverse site list.
	 */
	public final Iterator<Site> iterator() {
		return path.iterator();
	}
	
	/**
	 * Gives a string representation of a path.
	 * @return the string representing the path.
	 */
	@Override
	public final String toString() {
		StringBuffer strBuff = new StringBuffer();
		for (int i = 0; i < path.size(); i++) {
			strBuff.append(path.get(i).getID());
			if ((i + 1) < path.size()) {
				strBuff.append("->");
			}
		}
		return strBuff.toString();
	}

	/**
	 * Gets the last site in the path.
	 * @return the last site in the path, or null if the path is empty.
	 */
	public Site getLastSite() {
		if (path.size() > 0) {
			return path.get(path.size() - 1);
		} else {
			return null;
		}
	}

	public Site getFirstSite() {
		if (path.size() > 0) {
			return path.get(0);
		} else {
			return null;
		}
	}
	
}
