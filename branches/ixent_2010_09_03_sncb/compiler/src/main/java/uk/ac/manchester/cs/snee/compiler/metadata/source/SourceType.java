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
package uk.ac.manchester.cs.snee.compiler.metadata.source;

/**
 * Represents the type of data source
 *
 */
public enum SourceType {

	/** Source is a sensor network */
	SENSOR_NETWORK ("sensor network",true), 
	/** Source is a pull interface web service */
	PULL_STREAM_SERVICE ("pull-stream service source",false),
	/** Source is a push interface web service */
	PUSH_STREAM_SERVICE ("push-stream service source",false),
	/** Source is a UDP source */
	UDP_SOURCE ("UDP source", false),
	/** Data is a table with a relation (bag of tuples). */
	RELATIONAL ("DBMS",false),
	/** Source is a query end point */
	QUERY_SERVICE ("quere service source", true);
	
	/** Hold the string representation of the selected value. */
	private final String _stringRepresentation;
	
	/** True if this source can answer SNEEql queries. */
	private final boolean _queryable;

	/** Constructor.
	 * 
	 * @param stringRepresentaiton One of the Enum Values
	 */
	private SourceType(final String stringRepresentaiton, boolean queryable) {
		_stringRepresentation = stringRepresentaiton;
		_queryable = queryable;
	}	
	
	/**
	 * The Enum as a String.
	 * 
	 * @return String representation of this enum.
	 */ 
	public String toString() {
		return _stringRepresentation;
	}
	
	public boolean isQueryable() {
		return _queryable;
	}
	
}
