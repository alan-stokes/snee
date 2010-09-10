package uk.ac.manchester.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;

public class SNEEClientUsingInNetworkSource extends SNEEClient {
	
	private static uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator _myDataSource;

	public SNEEClientUsingInNetworkSource(String query, 
			double duration, String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingInNetworkSource()");		
		//Set sleep to 10 seconds
		_sleepDuration = 10000;
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClientUsingInNetworkSource()");
	}

	/**
	 * The main entry point for the SNEE in-network client.
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		//This method represents the web server wrapper
		// Configure logging
		PropertyConfigurator.configure(
				SNEEClientUsingInNetworkSource.class.
				getClassLoader().getResource("etc/log4j.properties"));
		String query;
		Long duration;
		String queryParams;
		if (args.length != 3) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n" +
					"\t\"query parameters file\"\n");
			//XXX: Use default query
			query = "SELECT * FROM PullStream;";
			duration = Long.valueOf("20");
			queryParams= "src/main/resources/etc/query-parameters.xml";
//			System.exit(1);
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
			queryParams = args[2];
		}
			
			try {
				/* Initialise SNEEClient */
				SNEEClientUsingInNetworkSource client = 
					new SNEEClientUsingInNetworkSource(query, duration, queryParams);
				/* Initialise and run data source */
				_myDataSource = new ConstantRatePushStreamGenerator();
				_myDataSource.startTransmission();
				/* Run SNEEClient */
				client.run();
				/* Stop the data source */
				_myDataSource.stopTransmission();
			} catch (Exception e) {
				System.out.println("Execution failed. See logs for detail.");
				logger.fatal(e);
				System.exit(1);
			}
//		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
