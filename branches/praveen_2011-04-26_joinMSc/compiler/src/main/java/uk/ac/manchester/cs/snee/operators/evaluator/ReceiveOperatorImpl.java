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
package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.UDPSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.WebServiceSourceMetadata;
import uk.ac.manchester.cs.snee.operators.evaluator.receivers.DummyUDPStreamReceiver;
import uk.ac.manchester.cs.snee.operators.evaluator.receivers.PullServiceReceiver;
import uk.ac.manchester.cs.snee.operators.evaluator.receivers.SourceReceiver;
import uk.ac.manchester.cs.snee.operators.evaluator.receivers.UDPStreamReceiver;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;

public class ReceiveOperatorImpl extends EvaluatorPhysicalOperator {
	//XXX: Write test for receive operator

	Logger logger = 
		Logger.getLogger(ReceiveOperatorImpl.class.getName());

	private SourceReceiver _streamReceiver;

	// streamName is the variable that stores the local extent name.
	private String _streamName;

	List<Attribute> attributes ;
	double rate;

	private ReceiveOperator receiveOp;

	ReceiveTimeoutException rte;

	private Timer _receiverTaskTimer;

	private long _longSleepPeriod;

	private long _shortSleepPeriod;
	
	private boolean isDummyReceiveEnabled = false;
	
	/**
	 * Instantiates the receive operator
	 * @param op
	 * @throws SchemaMetadataException 
	 * @throws SNEEConfigurationException 
	 */
	public ReceiveOperatorImpl(LogicalOperator op, int qid) 
	throws SchemaMetadataException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ReceiveOperatorImpl() for query " + 
					qid + " " +
					" with " + op);
		}
		
		// Instantiate this as a receive operator
		receiveOp = (ReceiveOperator) op;
		m_qid = qid;
		attributes = receiveOp.getInputAttributes();
		_streamName = receiveOp.getExtentName();
		rate = receiveOp.getStreamRate();
		try {
			isDummyReceiveEnabled = SNEEProperties
			.getBoolSetting(SNEEPropertyNames.DUMMY_RECEIVE_ENABLED);
		} catch (SNEEConfigurationException e) {
			isDummyReceiveEnabled = false;
			e.printStackTrace();
		}
	
		if (logger.isTraceEnabled()) {
			logger.trace("Receiver for stream: " + _streamName);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN ReceiveOperatorImpl()");
		}
	}

	@Override
	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		try {
			initializeStreamReceiver();

			// Start the receive operator to run immediately
			_receiverTaskTimer = new Timer();
			ReceiverTask task = new ReceiverTask();
			_receiverTaskTimer.schedule(task, 0 //initial delay
					,_longSleepPeriod);  //subsequent rate
		} catch (ExtentDoesNotExistException e) {
			throw new EvaluatorException(e);
		} catch (SourceMetadataException e) {
			throw new EvaluatorException(e);
		} catch (SNEEDataSourceException e) {
			throw new EvaluatorException(e);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	private void initializeStreamReceiver() 
	throws ExtentDoesNotExistException, SourceMetadataException, 
	SNEEDataSourceException {
		if(logger.isTraceEnabled()) {
			logger.trace("ENTER initializeStreamReceiver()");
		}
		SourceMetadataAbstract source = receiveOp.getSource();
		calculateSleepPeriods();
		SourceType sourceType = source.getSourceType();
		switch (sourceType) {
		case UDP_SOURCE:
			if (!isDummyReceiveEnabled) {
				instantiateUdpDataSource(source);
			} else  {
				instantiateDummyUdpDataSource(receiveOp, source);
			}
			break;
		case PULL_STREAM_SERVICE:
			instantiatePullServiceDataSource(source);
			break;
		default:
			String msg = "Unknown data source type " + sourceType;
			logger.warn(msg);
			throw new SourceMetadataException(msg);
		}
		//XXX: Mechanism above allows for more than one source, but streamReceiver is singular!
		_streamReceiver.open();
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN initializeStreamReceiver()");
		}
	}

	

	private void calculateSleepPeriods() 
	throws SourceMetadataException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER calculateSleepPeriod() for " + 
					_streamName + " with rate " + rate);
		}
		// Code to prevent rates being set to 0.0
		if (rate == 0.0) {
			try {
				rate = new Double(SNEEProperties.getSetting(
						SNEEPropertyNames.EVALUATOR_DEFAULT_POLL_RATE)).doubleValue();
			} catch (Exception e) {
				String message = "DEFAULT_POLL_RATE does not contain a double";
				logger.error(message);
				// If rate not set or incorrectly set, default to 1 tuple per second
				rate = 1.0;
			}
		}		
		double period = (1 / rate) * 1000;
		if (rate > 10) {
			//setting period to 1000 which is equivalant to 1 sec
			period = 1000;			
			//setting long sleep period to 
			_shortSleepPeriod = 1;			
		} else {
			//setting short sleep period to 0.1 of period
			_shortSleepPeriod = (long) (period * 0.1);			
		}
		//_shortSleepPeriod = (long) (period * 0.1);	
		_longSleepPeriod = (long) (period - _shortSleepPeriod);
		
		
		if (logger.isTraceEnabled()) {
			logger.trace(_streamName + " long sleep set to " +
					_longSleepPeriod + ", short sleep set to " +
					_shortSleepPeriod);
			logger.trace("RETURN calculateSleepPeriod()");
		}
	}

	private void instantiateUdpDataSource(SourceMetadataAbstract source)
	throws ExtentDoesNotExistException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER instantiateUdpDataSource() with " + 
					source.getSourceName());
		UDPSourceMetadata udpSource = (UDPSourceMetadata) source;
		_streamReceiver = new UDPStreamReceiver(
				udpSource.getHost(), 
				udpSource.getPort(_streamName), _shortSleepPeriod, 
				_streamName, attributes);
		if (logger.isTraceEnabled())
			logger.trace("RETURN instantiateUdpDataSource()");
	}

	private void instantiatePullServiceDataSource(SourceMetadataAbstract source) 
	throws ExtentDoesNotExistException, SNEEDataSourceException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER instantiatePullServiceDataSource() with " + 
					source.getSourceName());
		WebServiceSourceMetadata webSource = 
			(WebServiceSourceMetadata) source;
		_streamReceiver = new PullServiceReceiver(
				_streamName, webSource, _shortSleepPeriod);
		if (logger.isTraceEnabled())
			logger.trace("RETURN instantiatePullServiceDataSource()");
	}
	
	private void instantiateDummyUdpDataSource(ReceiveOperator receiveOp2, SourceMetadataAbstract source) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER instantiateDummyUdpDataSource() with " + 
					source.getSourceName());
		_streamReceiver = new DummyUDPStreamReceiver(receiveOp2, source);
		if (logger.isTraceEnabled())
			logger.trace("RETURN instantiateDummyUdpDataSource()");
	}

	@Override
	public void close(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		_receiverTaskTimer.cancel();
		_receiverTaskTimer.purge();
		// Close the stream
		_streamReceiver.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

	class ReceiverTask extends TimerTask {
	
		public void run(){
			if (logger.isDebugEnabled()) {
				logger.debug("ENTER run() for query " + m_qid);
			}
			List<Tuple> tuples = null ;
			//int counter = 1;
			/* 
			 * Process a tuple that has been received in the 
			 * background thread
			 */
			try {
				/*if (rate > 10) {
					counter = (int)rate;
				}*/
				//long currentTime = System.currentTimeMillis();
				//System.out.println("Counter is set to: "+ counter);
				List<TaggedTuple> taggedTuples = null;
				//for (int i = 0; i < counter; i++) {
					// receive the tuple, blocking operation					
					tuples = _streamReceiver.receive();
					
					
					// create a tagged tuple from the received tuple
					if (taggedTuples == null) {
						taggedTuples = new ArrayList<TaggedTuple>(tuples.size());
					}
					for (Tuple tuple : tuples) {
						TaggedTuple taggedTuple = new TaggedTuple(tuple);
						taggedTuples.add(taggedTuple);
					}		
					//If more tuples are generated or if a bunch of tuples are
					//generated together and the generation exceeds 1 sec break
					/*if (taggedTuples.size() > counter
							|| (counter > 10 && (System.currentTimeMillis() - currentTime) >= 1000)) {
						break;
					}*/
				//}
				System.out.println(_streamName+": "+taggedTuples.size());
				setChanged();
				notifyObservers(taggedTuples);
			} catch (ReceiveTimeoutException e) {
				logger.warn("Receive Timeout Exception.", e);
			} catch (SNEEException e) {
				logger.warn("Received a SNEEException.", e);
			} catch (EndOfResultsException e) {
			} catch (SNEEDataSourceException e) {
				logger.warn("Received a SNEEDataSourceException.", e);
			} catch (TypeMappingException e) {
				logger.warn("Received a TypeMappingException.", e);
			} catch (SchemaMetadataException e) {
				logger.warn("Received a SchemaMetadataException.", e);
			}
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN run() time to next execution: " + 
						_longSleepPeriod);
			}
		}
		
	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() for query " + m_qid);
		}
		logger.error("Receiver cannot be the parent of another operator");
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}
	
}
