package uk.ac.manchester.cs.snee.evaluator.operators.receivers;

import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.WebServiceSourceMetadata;
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class PullServiceReceiver implements SourceReceiver {

	Logger logger = 
		Logger.getLogger(PullServiceReceiver.class.getName());

	private String _resourceName;

	private PullSourceWrapper _pullSource;

	private Timestamp lastTs;

	private long _sleep;

	public PullServiceReceiver(String streamName,
			WebServiceSourceMetadata webSource, long sleep) 
	throws ExtentDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER PullServiceReceiver() with " + 
					streamName + " sleep duration=" + sleep);
		_resourceName = webSource.getResourceName(streamName);
		_pullSource = webSource.getPullSource();
		_sleep = sleep;
		if (logger.isDebugEnabled())
			logger.debug("RETURN PullServiceReceiver()");
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER open()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN open()");
	}

	@Override
	public Tuple receive() 
	throws EndOfResultsException, SNEEDataSourceException, 
	TypeMappingException, SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER receive()");
		Tuple tuple = null;
		if (lastTs == null) {
			List<Tuple> tuples = 
				_pullSource.getNewestData(_resourceName, 1);
			tuple = tuples.get(0);
		} else {
			List<Tuple> tuples = 
				_pullSource.getData(_resourceName, 2, lastTs);
			while (tuples.size() < 2) {
				try {
					logger.trace("No new tuple yet. Sleep for " + _sleep);
					//Sleep for 10s before trying again
					Thread.sleep(_sleep);
				} catch (InterruptedException e1) {
					logger.warn("InterruptionException " + e1);
				}
				tuples = _pullSource.getData(_resourceName, 2, lastTs);
			}
			tuple = tuples.get(1);
		}
		//FIXME:This is very CCO dependent!!!
		long sqlTsValue = ((Integer) tuple.getValue("timestamp")).longValue();
		lastTs =  new Timestamp(sqlTsValue*1000);
		if (logger.isTraceEnabled()) {
			logger.trace("timestamp " + lastTs + " " +
					lastTs.getTime());
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN receive() with " + tuple);
		return tuple;
	}

}
