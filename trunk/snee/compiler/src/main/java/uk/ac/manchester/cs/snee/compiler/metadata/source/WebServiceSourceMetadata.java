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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;

/**
 * Implementation of the metadata imported from a web service source.
 */
public class WebServiceSourceMetadata extends PullSourceMetadata {

	Logger logger = 
		Logger.getLogger(WebServiceSourceMetadata.class.getName());
	
	private String _url;

	/**
	 * Resource names stored by extentName
	 */
	private Map<String, String> _resources;

	private PullSourceWrapper _pullSource;

	public WebServiceSourceMetadata(String sourceName, 
			List<String> extentNames, String url, 
			Map<String, String> resources, PullSourceWrapper pullSource) {
		super(sourceName, extentNames, SourceType.PULL_STREAM_SERVICE);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER WebServiceSourceMetadata() with " + url);
		}
		try {
			_sourceName = (new URL(url)).getHost();
		} catch (MalformedURLException e) {
			_sourceName = "";
		}
		_url = url;
		_resources = resources;
		_pullSource = pullSource;
		setStreamRates();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN WebServiceSourceMetadata() " + this);
		}
	}

	private void setStreamRates() {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setStreamRates()");
		for (String extentName : _extentNames) {
			//FIXME: Read rate from stream property document
			double rate;
			if (extentName.endsWith("met") ||
					extentName.endsWith("tide")) {
				rate = 1/(60.0*10.0);
			} else {
				rate = 1/(60.0 * 30.0);
			}
			setRate(extentName, rate);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN setStreamRates()");
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

	public PullSourceWrapper getPullSource() {
		return _pullSource;
	}

}
