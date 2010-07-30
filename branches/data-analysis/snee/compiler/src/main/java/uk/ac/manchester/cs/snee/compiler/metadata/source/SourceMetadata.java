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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Generic information known about all data sources
 */
public abstract class SourceMetadata {

	Logger logger = Logger.getLogger(SourceMetadata.class.getName());

	/**
	 * Name used to identify the source
	 */
	protected String _sourceName;
	
	/**
	 * The names of the extents available from this source
	 */
	protected List<String> _extentNames = new ArrayList<String>();

	protected SourceType _sourceType;

	/**
	 * Instantiates a source metadata object to caputre common source
	 * metadata
	 * @param sourceName name used to identify the source
	 * @param extentNames names of the extents available from this source
	 * @param sourceType the type of the source
	 */
	public SourceMetadata(String sourceName, 
			List<String> extentNames, SourceType sourceType) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourceMetadata() with " + sourceName +
					" extents " + extentNames +
					" type " + sourceType);
		_sourceName = sourceName;
		_extentNames = extentNames;
		_sourceType = sourceType;
		if (logger.isDebugEnabled())
			logger.debug("RETURN SourceMetadata()");
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object ob) {
		boolean result = false;
		if (ob instanceof SourceMetadata) {
			SourceMetadata source = (SourceMetadata) ob;
			/*
			 * Equality assumed if source refer to the same source type
			 * and the same extents.
			 */
			if (logger.isTraceEnabled())
				logger.trace("Testing " + source + "\nwith " + this);
			if (source.getExtentNames().equals(_sourceName) &&
					source.getSourceType() == _sourceType)
					result = true;
		}
		return result;
	}

	/**
	 * Get the name of the source
	 * @return source name
	 */
	public String getSourceName() {
		return _sourceName;
	}

	/**
	 * Returns a list of the extent names for which this source 
	 * provides data
	 * @return
	 */
	public List<String> getExtentNames() {
		return _extentNames;
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(_sourceName);
		s.append("  Extent Type: " + _sourceType);
		return s.toString();
	}

	/**
	 * Returns true if this source provides streaming data.
	 * @return true if this source provides streaming data
	 */
	public boolean isStream() {
		if (_sourceType == SourceType.PULL_WEB_SERVICE || 
				_sourceType == SourceType.UDP_SOURCE ||
				_sourceType == SourceType.SENSOR_NETWORK) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns the type of the source.
	 * @return the source type
	 */
	public SourceType getSourceType() {
		return _sourceType;
	}

}
