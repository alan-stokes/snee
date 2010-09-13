/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
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
package uk.ac.manchester.cs.snee.evaluator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.sncb.SerialPortMessageReceiver;

public class InNetworkQueryEvaluator extends QueryEvaluator {//Runnable {

	private Logger logger = 
		Logger.getLogger(InNetworkQueryEvaluator.class.getName());

	private SerialPortMessageReceiver _serialPortMessageReceiver;
	
	protected InNetworkQueryEvaluator() {
		// Constructor for mock object test purposes
	}
	
	public InNetworkQueryEvaluator(int queryId, SerialPortMessageReceiver spmr,
			ResultStore resultSet) 
	throws SNEEException, SchemaMetadataException, EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER InNetworkQueryEvaluator() with queryID: " + queryId);
		}
		this._queryId = queryId;
		_serialPortMessageReceiver = spmr;
		_serialPortMessageReceiver.addObserver(this);
		this._results = resultSet;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN InNetworkQueryEvaluator()");
		}
	}

	public void stopExecuting() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER stopExecuting() on query " + _queryId);
		}
		// Protected to prevent null pointer exception
		if (_serialPortMessageReceiver != null && executing) {
			// Stop the query evaluation
			_serialPortMessageReceiver.close();
		}
		executing = false;	
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN stopExecuting()");
		}
	}
}
