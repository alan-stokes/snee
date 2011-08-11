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
package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.math.BigDecimal;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;

public class UDPStreamReceiver implements SourceReceiver {

	Logger logger = Logger.getLogger(this.getClass().getName());

	private String _host;
	private int _port;
	private MulticastSocket _socket;
	private InetAddress _inetAddress;

	private long _sleep;

	private List<Attribute> attributes;

	private String streamName;

	/**
	 * Creates a UDPStreamReceiverHelper for an event stream source
	 * 
	 * @param pHost Host where the stream originates
	 * @param pPort Port on which to listen for the stream
	 * @param attributes TODO
	 * @param shortSleepPeriod 
	 */
	public UDPStreamReceiver(String pHost, int pPort, long sleepPeriod, 
			String streamName, List<Attribute> attributes) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER UDPStreamReceiver() " + pHost + 
					":" + pPort);
		}
		logger.trace("Operator helper for an event stream");
		_host = pHost;
		_port = pPort;
		_sleep = sleepPeriod;
		this.attributes = attributes;
		this.streamName = streamName;
		try {
			_inetAddress = InetAddress.getByName(pHost);
		} catch (UnknownHostException e) {
			logger.error(e.getMessage());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN UDPStreamReceiver()");
		}
	}

	public String getHost() {
		return _host;
	}

	public int getPort() {
		return _port;
	}

	public void open(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		logger.trace("Opening a stream");
		try {
			_socket = new MulticastSocket(_port);
			_socket.joinGroup(_inetAddress);
			_socket.setSoTimeout(
					EvaluatorPhysicalOperator.RECEIVE_TIMEOUT);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	/**
	 * Receive a tuple over the socket connection
	 * @return
	 * @throws EndOfResultsException 
	 * @throws SNEEDataSourceException 
	 * @throws ReceiveTimeoutException 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public Tuple receive() 
	throws EndOfResultsException, SNEEDataSourceException, 
	ReceiveTimeoutException, SchemaMetadataException,
	TypeMappingException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER receive()");
		}
		Tuple tuple = null;
		try {
			while (tuple == null) {
				byte[] b = new byte[65535];
				ByteArrayInputStream b_in = new ByteArrayInputStream(b);
				DatagramPacket dgram = new DatagramPacket(b, b.length);
				try {
					_socket.receive(dgram); // blocks
				} catch (SocketTimeoutException e) {
					logger.warn("Socket timed out. " + e);
					throw new ReceiveTimeoutException(_socket.getSoTimeout());
				}
				ObjectInputStream o_in = new ObjectInputStream(b_in);
				if (logger.isTraceEnabled()) {
					logger.trace("Receiving a tuple");
				}
				String dataCSV = (String) o_in.readObject();
				tuple = convertCSVToTuple(dataCSV);
				dgram.setLength(b.length); // must reset length field!
				b_in.reset(); // reset so next read is from start of byte[] again
				try {
					Thread.sleep(_sleep);
				} catch (InterruptedException e) {
				}
			}
		} catch (IOException e) {
			logger.warn(e.getMessage());
			throw new SNEEDataSourceException(e);
		} catch (ClassNotFoundException e) {
			logger.warn("Unable to reform tuple. " + e);
			throw new SNEEDataSourceException("Unable to reform tuple.", e);
		}
		if (_socket.isClosed()) {
			if (logger.isTraceEnabled()) {
				logger.trace("Socket closed!");
			}
			logger.warn("End of stream!");
			throw new EndOfResultsException();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN receive() " + tuple);
		}
		return tuple;
	}

	private Tuple convertCSVToTuple(String dataCSV) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER convertCSVToTuple() with " + dataCSV);
		}
		StringTokenizer st = new StringTokenizer(dataCSV, ",");
		int col = 0;
		List<EvaluatorAttribute> evalAttributes = 
			new ArrayList<EvaluatorAttribute>();
		while (st.hasMoreTokens()) {
			Attribute attribute = attributes.get(col);
			EvaluatorAttribute evalAttr = 
				new EvaluatorAttribute(streamName, 
								attribute.getAttributeSchemaName(),
								attribute.getAttributeDisplayName(),
								attribute.getType(), 
						convertStringToType(st.nextToken(), 
								attribute.getType().getName()));
			evalAttributes.add(evalAttr);
			col++;
		}
		Tuple tuple = new Tuple(evalAttributes);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN convertCSVToTuple() with " + tuple);
		}
		return tuple;
	}

	private Object convertStringToType(String nextToken, 
			String typeName) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER convertStringToType() with " +
					nextToken + " " + typeName);
		}
		Object obj;
		if (typeName.equals("boolean")) {
			obj = new Boolean(nextToken);
		} else if (typeName.equals("decimal")) {
			obj = new BigDecimal(nextToken);
		} else if (typeName.equals("float")) {
			obj = new Float(nextToken);
		} else if (typeName.equals("integer")) {
			obj = new Integer(nextToken);
		} else if (typeName.equals("string")) {
			obj = nextToken;
		} else if (typeName.equals("timestamp")) {
			obj = new Long(nextToken);
		} else {
			obj = nextToken;
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN convertStringToType() with " + obj);
		}
		return obj;
	}

	public void close(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		_socket.disconnect();
		if (logger.isTraceEnabled()) {
			logger.trace("Socket disconnected");
		}
		_socket.close();
		if (logger.isTraceEnabled()) {
			logger.trace("Socket closed");
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

}
