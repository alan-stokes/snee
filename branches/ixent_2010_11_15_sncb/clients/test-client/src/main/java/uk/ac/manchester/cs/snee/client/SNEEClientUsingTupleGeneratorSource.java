package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;

public class SNEEClientUsingTupleGeneratorSource extends SNEEClient {
	
	private static uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator _myDataSource;

	public SNEEClientUsingTupleGeneratorSource(String query, 
			double duration) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClietnUsingTupleGeneratorSource()");		
		//Set sleep to 10 seconds
		_sleepDuration = 10000;
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClietnUsingTupleGeneratorSource()");
	}

	/**
	 * The main entry point for the SNEE controller
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		// Configure logging
		PropertyConfigurator.configure(
				SNEEClientUsingTupleGeneratorSource.class.
				getClassLoader().getResource("etc/log4j.properties"));
		Long duration;
		String query;
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
			//XXX: Use default query

			query = "SELECT * FROM PushStream;";

			/* The following query is successfully executed */
//			query = 
//				"SELECT p.intattr, s.integerColumn, s.floatColumn " +
//				"FROM PushStream[NOW] p, " +
//				"	(SELECT intattr as integerColumn, floatattr as floatColumn FROM PushStream[NOW]) s;";

//			query = "SELECT ps.myint " +
//					"FROM ( SELECT ts.intattr AS myint FROM PushStream ts) ps;";
//			query = "SELECT avg(*) FROM ( SELECT ps.intattr AS myint FROM PushStream ps);";
//			query = "SELECT COUNT(intattr) FROM PushStream GROUP BY intattr;";
//			query = "SELECT * FROM ( SELECT * FROM PushStream ps);";
			duration = Long.valueOf("20");
//			System.exit(1);
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
		}
			
			try {
				/* Initialise SNEEClient */
				SNEEClientUsingTupleGeneratorSource client = 
					new SNEEClientUsingTupleGeneratorSource(query, duration);
				/* Initialise and run data source */
				_myDataSource = new ConstantRatePushStreamGenerator();
				_myDataSource.startTransmission();
				/* Run SNEEClient */
				client.run();
				/* Stop the data source */
				_myDataSource.stopTransmission();
			} catch (Exception e) {
				System.out.println("Execution failed. See logs for detail.");
				logger.fatal("Execution failed", e);
				System.exit(1);
			}
//		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
