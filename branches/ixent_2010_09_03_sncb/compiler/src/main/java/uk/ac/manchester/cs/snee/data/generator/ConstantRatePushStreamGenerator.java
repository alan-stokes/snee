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
package uk.ac.manchester.cs.snee.data.generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.metadata.source.UDPSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

/**
 * @author agray
 *
 */
public class ConstantRatePushStreamGenerator {

	Logger logger = 
		Logger.getLogger(ConstantRatePushStreamGenerator.class.getName());
	
	/**
	 * The list of streams
	 */
	List<ExtentMetadata> _streams;

	/**
	 * The generators which create tuples
	 */
	Map<String, TupleGenerator> generators = 
		new HashMap<String, TupleGenerator>();

	/**
	 * The sockets for the streams to be sent across
	 */
	Map<String, MulticastSocket> sockets =
		new HashMap<String, MulticastSocket>();

	/**
	 * The timers which control the threads which generate the streams
	 */
	private List<Timer> _timers;
	private List<BroadcastTask> _tasks;


	/**
	 * The constructor initialises the stream generator.
	 * @throws TypeMappingException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws SchemaMetadataException 
	 * @throws MetadataException 
	 * @throws SourceMetadataException 
	 * @throws SNEEConfigurationException 
	 * @throws TopologyReaderException 
	 * @throws SNEEDataSourceException 
	 * @throws MalformedURLException 
	 * @throws CostParametersException 
	 */
	public ConstantRatePushStreamGenerator() 
	throws TypeMappingException, MetadataException, 
	SchemaMetadataException, UnsupportedAttributeTypeException,
	SourceMetadataException, SNEEConfigurationException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ConstantRatePushStreamGenerator()");
		}
		Types types = new Types(SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE));
		Metadata schema = 
			new Metadata();
		_streams = schema.getPushedExtents();
		_timers = new ArrayList<Timer>();
		_tasks = new ArrayList<BroadcastTask>();
		
		// Create a tuple generator and a socket for each stream
		for (ExtentMetadata stream : _streams) {
			String streamName = stream.getExtentName().toLowerCase();
			TupleGenerator tupleGeneator = 
				new TupleGenerator(stream, types);
			generators.put(streamName, tupleGeneator);
			List<SourceMetadata> sources = schema.getSources(streamName);
			initialiseSources(stream, streamName, sources);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN ConstantRatePushStreamGenerator() " +
					"#streams=" + _streams.size());
		}
	}

	private void initialiseSources(ExtentMetadata stream, 
			String streamName, List<SourceMetadata> sources) 
	throws ExtentDoesNotExistException, SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER initialiseSources() for " + streamName +
					" #sources=" + sources.size());
		for (SourceMetadata source : sources) {
			if (source.getSourceType() == SourceType.UDP_SOURCE) {
				UDPSourceMetadata udpSource = (UDPSourceMetadata) source;
				instantiateUDPSource(udpSource, streamName);
				_tasks.add(createBroadcastTask(stream, udpSource));
			}
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN initialiseSources() " +
					"#tasks=" + _tasks.size());
	}

	private void instantiateUDPSource(UDPSourceMetadata source, 
			String streamName) 
	throws ExtentDoesNotExistException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER instantiateUDPSource() with " + 
					source.getSourceName());
		try {
			MulticastSocket socket = new MulticastSocket(source.getPort(streamName));
			socket.joinGroup(InetAddress.getByName(source.getHost()));
			sockets.put(streamName, socket);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN instantiateUDPSource()");
	}

	private BroadcastTask createBroadcastTask(ExtentMetadata stream, 
			UDPSourceMetadata udpSource) 
	throws ExtentDoesNotExistException, SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createBroadcastTask() for " + 
					stream.getExtentName());
		String streamName = stream.getExtentName().toLowerCase();
		Timer timer = new Timer();
		BroadcastTask task = new BroadcastTask();

		// Set variables of the task
		task.streamName = streamName;
		MulticastSocket socket = sockets.get(streamName);
		if (logger.isTraceEnabled())
			logger.trace("Socket: " + socket);
		task.mcSocket = socket;
		String hostAddress = udpSource.getHost();
		if (logger.isTraceEnabled()) {
			logger.trace("Host: " + hostAddress);
		}
		task.address = hostAddress;
		int port = udpSource.getPort(streamName);
		logger.trace("Port: " + port);
		task.port = port;
		task.tupleGenerator = generators.get(streamName);
		task.timer = timer;
		task.sleepInterval = 
			calculateSleepInterval(udpSource.getRate(streamName));		
		if (logger.isTraceEnabled())
			logger.trace("RETURN createBroadcastTask()");
		return task;
	}

	/**
	 * Creates a task for each stream to send tuples over a socket at the
	 * desired rate
	 * @throws SchemaMetadataException 
	 */
	public void startTransmission() throws SchemaMetadataException {
		logger.debug("ENTER startTransmission()");
		Timer timer;
		logger.debug("Here with streams " + _streams.size());
		for (BroadcastTask task : _tasks) {
			timer = new Timer();
			// Set schedule for the task
			timer.schedule(task, 0, //initial delay
					task.sleepInterval); //subsequent rate
			// Add timer to the set of timers
			_timers.add(timer);
			logger.debug("Started stream: " + task.streamName + " host: " +
					task.address + ":" + task.port);

		}
		logger.debug("RETURN startTransmission() #timers=" + _timers.size());
	}

	/**
	 * Calculate the period to wait between each generated tuple
	 * @param rate
	 * @return
	 */
	private int calculateSleepInterval(double rate) {
		return (int) (1000/rate);
	}

	/**
	 * Stop each of the tasks that is generating a stream
	 */
	public void stopTransmission() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER stopTransmission() " +
				"Number of active timers: " + _timers.size());
		}
		for (Timer timer : _timers) {
			if (logger.isTraceEnabled()) {
				logger.trace("Stopping timer " + timer.toString());
			}
			timer.cancel();
			timer.purge();
		}
		_timers.removeAll(_timers);
		try {
			// Give the threads a chance to end
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN stopTransmission() " +
					"Number of active timers: " + _timers.size());
		}
	}

	class BroadcastTask extends TimerTask {
		private TupleGenerator tupleGenerator;
		private int curIndex = 0;
		private MulticastSocket mcSocket;
		private String streamName;
		private int port;
		private String address;
		private Timer timer;
		private int sleepInterval;

		public void run() {
			curIndex++;
			try {
				InetAddress inetAddress = InetAddress.getByName(address);
				// Generate next tuple for the stream
				Tuple tuple = tupleGenerator.generateTuple(curIndex);
				
				if (logger.isTraceEnabled())
					logger.trace("Creating byte stream for tuple " + tuple);
				// Create a byte stream
				ByteArrayOutputStream b_out = new ByteArrayOutputStream();
				ObjectOutputStream o_out = new ObjectOutputStream(b_out);

				// Create a byte array out of the new tuple
				o_out.writeObject(tuple);
				byte[] b = b_out.toByteArray();

				if (logger.isTraceEnabled())
					logger.trace("Creating packet for " + address + ":" +
							port);
				// Create a UDP packet to send the tuple
				DatagramPacket packet = new DatagramPacket(b, b.length,
						inetAddress, port);
								
				// Send tuple over the socket
				if (logger.isTraceEnabled())
					logger.trace("Sending tuple");
				mcSocket.send(packet);
				if (logger.isTraceEnabled()) {
					logger.trace(streamName + " - " + 
							tuple.toString());
				}
			} catch (UnknownHostException e) {
				logger.warn("Error creating connection for transmission. " + e);
			} catch (IOException e) {
				logger.warn("Error sending tuple. " + e);
			} catch (Exception e) {
				logger.warn("Unable to generate tuples. " + e);
				cancel();
				if (logger.isTraceEnabled())
					logger.trace("Removing from set of timers " + timer);
				_timers.remove(timer);
			}
		}
	}

}
