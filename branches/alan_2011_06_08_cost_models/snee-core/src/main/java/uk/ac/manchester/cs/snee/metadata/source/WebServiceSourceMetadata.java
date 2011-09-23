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
package uk.ac.manchester.cs.snee.metadata.source;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.datasource.webservice.SourceWrapper;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;

/**
 * Implementation of the metadata imported from a web service source.
 */
public class WebServiceSourceMetadata extends SourceMetadata {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 6850767324052003664L;

  private static final Logger logger = Logger.getLogger(WebServiceSourceMetadata.class.getName());
	
	private String _url;

	/**
	 * Resource names stored by extentName
	 */
	private Map<String, String> _resources;

	private SourceWrapper _source;

	public WebServiceSourceMetadata(String sourceName, 
			List<String> extentNames, String url, 
			Map<String, String> resources, SourceWrapper sourceWrapper) {
		super(sourceName, extentNames, sourceWrapper.getSourceType());
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER WebServiceSourceMetadata() with " + url);
		}
		if (sourceName == null || sourceName.equals("")) {
			try {
				_sourceName = (new URL(url)).getHost();
			} catch (MalformedURLException e) {
				_sourceName = "";
			}
		} else {
			_sourceName = sourceName;
		}
		_url = url;
		_resources = resources;
		_source = sourceWrapper;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN WebServiceSourceMetadata() " + this);
		}
	}

//	public boolean equals(Object ob) {
//		boolean result = false;
//		if (ob instanceof WebServiceSourceMetadata) {
//			WebServiceSourceMetadata source = (WebServiceSourceMetadata) ob;
//			//XXX: Not necessarily a complete check of source metadata equality
//			/*
//			 * Equality assumed if source refer to the same source type
//			 * and the same extent.
//			 */
//			if (logger.isTraceEnabled())
//				logger.trace("Testing " + source + "\nwith " + this);
//			if (source.getName().equals(_name) &&
//					source.getSourceType() == _sourceType)
//					result = true;
//		}
//		return result;
//	}
	
	public String getServiceUrl() {
		return _url;
	}
	
	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
		s.append("\tURL " + _url);
		return s.toString();
	}

	/**
	 * Retrieve the resource that provides a specified extent
	 * @param extentName name of the extent
	 * @return resource name
	 * @throws ExtentDoesNotExistException extent name is unknown to service
	 */
	public String getResourceName(String extentName) 
	throws ExtentDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getResourceName() with extent " + 
					extentName);
		String resourceName;
		if (_resources.containsKey(extentName)) {
			resourceName = _resources.get(extentName);
		} else {
			String msg = "Unknown extent " + extentName;
			logger.warn(msg);
			throw new ExtentDoesNotExistException(msg);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getResourceName() with " + resourceName);
		return resourceName;
	}

	public SourceWrapper getSource() {
		return _source;
	}

}
