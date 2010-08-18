package uk.ac.manchester.cs.snee.client;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEE;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.StreamResultSet;
import uk.ac.manchester.cs.snee.StreamResultSetImpl;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;

public abstract class SNEEClient implements Observer {

	protected static Logger logger = 
		Logger.getLogger(SNEEClient.class.getName());
	protected SNEE controller;
	protected String _query;
	protected double _duration;
	protected long _sleepDuration;

	public SNEEClient(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEClient() with query " + query + 
					" duration " + duration);
		
		_query = query;
		_duration = duration;
		controller = new SNEEController("etc/snee.properties");

		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClient()");
	}

	private static void printResults(Collection<Output> results, 
			int queryId) {
		System.out.println("************ Results for query " + 
				queryId + " ************");
		for (Output output : results) {
			System.out.println(output);
		}
		System.out.println("*********************************");
	}

	@SuppressWarnings("unchecked")
	public void update (Observable observation, Object arg) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() with " + observation + " " + 
					arg);
		logger.trace("arg type: " + arg.getClass());
		if (arg instanceof Collection<?>) {
			Collection<Output> results = (Collection<Output>) arg;
			printResults(results, 1);
		} else if (arg instanceof Output) {
			Output output = (Output) arg;
			System.out.println(output);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}
	
	public void run() 
	throws SNEECompilerException, MetadataException, EvaluatorException,
	SNEEException {
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER");
		System.out.println("Query: " + this._query);

		//		try {
		int queryId1 = controller.addQuery(_query, null);
		//		int queryId2 = controller.addQuery(query);

		long startTime = System.currentTimeMillis();
		long endTime = (long) (startTime + (_duration * 1000));

		System.out.println("Running query for " + _duration + 
			" seconds.");

		StreamResultSetImpl resultSet = 
			(StreamResultSetImpl) controller.getResultSet(queryId1);
		resultSet.addObserver(this);
		
		try {			
			Thread.currentThread().sleep((long)_duration * 1000);
		} catch (InterruptedException e) {
		}
		
		while (System.currentTimeMillis() < endTime) {
			Thread.currentThread().yield();
		}
		
		List<Output> results1 = displayFinalResult(queryId1);

		try {
			//XXX: Sleep included to highlight evaluator not ending bug 
			Thread.currentThread().sleep((long) ((_duration/2) * 1000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		controller.close();
		printResults(results1, queryId1);
		//		printResults(results2, queryId2);
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	private List<Output> displayFinalResult(int queryId1) throws SNEEException {
		StreamResultSet resultSet1 = controller.getResultSet(queryId1);
		List<Output> results1 = resultSet1.getResults();
		//		Collection<Output> results2 = controller.getResults(queryId2);
		System.out.println("Stopping query " + queryId1 + ".");
		controller.removeQuery(queryId1);
		//		System.out.println("Query run for required duration. " +
		//				"Stopping query " + queryId2 + ".");
		//		controller.removeQuery(queryId2);
		return results1;
	}

}