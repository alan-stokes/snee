package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapperImpl;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.WebServiceSourceMetadata;

public class PullServiceReceiver implements SourceReceiver {

	Logger logger = 
		Logger.getLogger(PullServiceReceiver.class.getName());

	private String _resourceName;

	private PullSourceWrapperImpl _pullSource;

	private Timestamp lastTs;

	private long _sleep;

	private String extentName;

	public PullServiceReceiver(String streamName,
			WebServiceSourceMetadata webSource, long sleep) 
	throws ExtentDoesNotExistException, SNEEDataSourceException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER PullServiceReceiver() with " + 
					streamName + " sleep duration=" + sleep);
		extentName = streamName;
		_resourceName = webSource.getResourceName(streamName);
		if (webSource.getSourceType() == SourceType.PULL_STREAM_SERVICE) {
			_pullSource = (PullSourceWrapperImpl) webSource.getSource();
		} else {
			String message = "Incorrect data source type.";
			logger.error(message);
			throw new SNEEDataSourceException(message);
		}
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
	public List<Tuple> receive() 
	throws EndOfResultsException, SNEEDataSourceException, 
	TypeMappingException, SchemaMetadataException, SNEEException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER receive()");
		List<Tuple> tuples = new ArrayList<Tuple>();
		if (lastTs == null) {
			tuples = getNewestTuple();
		} else {
			tuples = getNextTuples();
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Number of tuples received: " + tuples.size());
		}
		//XXX: Hack to correct tuple names provided by CCO
		tuples = correctTableName(tuples);

		updateLastSeenTimestamp(tuples);
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN receive() with " + tuples.size());
		return tuples;
	}

	private void updateLastSeenTimestamp(List<Tuple> tuples) 
	throws AssertionError, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER updataLastSeenTimestamp() #tuples=" + tuples.size());
		}
		long sqlTsValue;
		Tuple tuple = tuples.get(tuples.size() - 1);
		Object value = tuple.getAttributeValue(extentName, "timestamp");
		if (value instanceof Long) {
			sqlTsValue = (Long) value;
		} else if (value instanceof Integer) {
			sqlTsValue = ((Integer) tuple.getAttributeValue(extentName, 
					"timestamp")).longValue();
		} else {
			logger.error("Unsupported object type for timestamp: " + 
					value.getClass());
			throw new AssertionError();
		}
		lastTs =  new Timestamp(sqlTsValue*1000);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN updataLastSeenTimestamp() with " +
					extentName + ".timestamp " + lastTs + " " +
					lastTs.getTime());
		}
	}

	private List<Tuple> getNextTuples() throws SNEEDataSourceException,
			TypeMappingException, SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getNextTuples()");
		}
		List<Tuple> tuples = 
			_pullSource.getData(_resourceName, lastTs);
		while (tuples.isEmpty() || tuples.size() < 2) {
			try {
				logger.trace("No new tuple yet. Sleep for " + _sleep);
				Thread.sleep(_sleep);
			} catch (InterruptedException e1) {
				logger.warn("InterruptionException " + e1);
			}
			tuples = _pullSource.getData(_resourceName, lastTs);
		}
		logger.trace("Removing already seen tuple!");
		/*
		 * Remove the already seen tuple. Requesting by timestamp means that 
		 * we have already seen one of the tuples that we've just received.
		 */
		tuples.remove(0);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getNextTuples() with " + tuples.size());
		}
		return tuples;
	}

	private List<Tuple> getNewestTuple() throws SNEEDataSourceException,
			TypeMappingException, SchemaMetadataException, SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getNewestTuple()");
		}
		List<Tuple> tuples = _pullSource.getNewestData(_resourceName, 1);
		while (tuples.isEmpty()) {
			try {
				logger.trace("No tuple yet. Sleep for " + _sleep);
				Thread.sleep(_sleep);
			} catch (InterruptedException e1) {
				logger.warn("InterruptionException " + e1);
			}
			tuples = _pullSource.getNewestData(_resourceName, 1);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getNewestTuple() with " + tuples.size());
		}
		return tuples;
	}

	/**
	 * Hack to overcome shortcomings in metadata inserted to 
	 * WebRowSet object by the CCO-WS.
	 * 
	 * @param tuple
	 * @return
	 * @throws SchemaMetadataException
	 */
	private List<Tuple> correctTableName(List<Tuple> tuples) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER correctTableName() with " + tuples.size());
		}
		List<Tuple> correctedTuples = new ArrayList<Tuple>();
		for (Tuple tuple : tuples) {
			Tuple correctedTuple = new Tuple();
			for (EvaluatorAttribute attr : tuple.getAttributeValues()) {
				EvaluatorAttribute correctAttr = 
					new EvaluatorAttribute(extentName, 
							attr.getAttributeSchemaName(), 
							attr.getAttributeDisplayName(), 
							attr.getType(), 
							attr.getData());
				correctedTuple.addAttribute(correctAttr);
			}
			if (logger.isTraceEnabled()) {
				logger.trace("Correct tuple: " + correctedTuple);
			}
			correctedTuples.add(correctedTuple);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN correctTableName() #tuples=" + 
					correctedTuples.size());
		}
		return correctedTuples;
	}

}
