package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.WebServiceSourceMetadata;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class PullServiceReceiver implements SourceReceiver {

	Logger logger = 
		Logger.getLogger(PullServiceReceiver.class.getName());

	private String _resourceName;

	private PullSourceWrapper _pullSource;

	private Timestamp lastTs;

	private long _sleep;

	private String extentName;

	public PullServiceReceiver(String streamName,
			WebServiceSourceMetadata webSource, long sleep) 
	throws ExtentDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER PullServiceReceiver() with " + 
					streamName + " sleep duration=" + sleep);
		extentName = streamName;
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
			//XXX: Hack here as CCO returns table name as temp!
			tuple = correctTableName(tuples.get(0));
		} else {
			List<Tuple> tuples = 
				_pullSource.getData(_resourceName, 2, lastTs);
			while (tuples.size() < 2) {
				try {
					logger.trace("No new tuple yet. Sleep for " + 
							_sleep);
					//Sleep for 10s before trying again
					Thread.sleep(_sleep);
				} catch (InterruptedException e1) {
					logger.warn("InterruptionException " + e1);
				}
				tuples = _pullSource.getData(_resourceName, 2, lastTs);
			}
			tuple = correctTableName(tuples.get(1));
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Received tuple: \n" + tuple);
		}
		//TODO: This is very CCO dependent!!!
		/* 
		 * In the CCO schema, the 'timestamp' attribute is declared
		 * to be of type integer!
		 * This is why the cast below is to type Integer.
		 */
		long sqlTsValue = 
			((Integer) tuple.getAttributeValue(extentName, 
					"timestamp")).longValue();
		lastTs =  new Timestamp(sqlTsValue*1000);
		if (logger.isTraceEnabled()) {
			logger.trace(extentName + ".timestamp " + lastTs + " " +
					lastTs.getTime());
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN receive() with " + tuple);
		return tuple;
	}

	/**
	 * Hack to overcome shortcomings in metadata inserted to 
	 * WebRowSet object by the CCO-WS.
	 * 
	 * @param tuple
	 * @return
	 * @throws SchemaMetadataException
	 */
	private Tuple correctTableName(Tuple tuple) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER correctTableName() with\n" + tuple);
		}
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
			logger.trace("RETURN correctTableName() with\n" + 
					correctedTuple);
		}
		return correctedTuple;
	}

}
