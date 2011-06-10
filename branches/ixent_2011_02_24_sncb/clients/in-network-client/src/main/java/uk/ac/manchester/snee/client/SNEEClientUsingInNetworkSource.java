package uk.ac.manchester.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;

public class SNEEClientUsingInNetworkSource extends SNEEClient {

	public SNEEClientUsingInNetworkSource(String query, 
			double duration, String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingInNetworkSource()");		
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
			query = "SELECT * FROM SeaDefence;";
//			query = "SELECT seaLevel, \'East\' FROM SeaDefence;";
			//query = "SELECT avg(seaLevel) FROM SeaDefence;";
			//query = "SELECT e.seaLevel FROM SeaDefenceEast[NOW] e, SeaDefenceWest[NOW] w WHERE e.seaLevel > w.seaLevel;";
			duration = Long.valueOf("120");
			queryParams= "etc/query-parameters.xml";
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
			queryParams = args[2];
		}
			
		try {
			/* Initialise SNEEClient */
			SNEEClientUsingInNetworkSource client = 
				new SNEEClientUsingInNetworkSource(query, duration, queryParams);
			/* Run SNEEClient */
			client.run();
		} catch (Exception e) {
			System.out.println("Execution failed. See logs for detail.");
			logger.fatal(e);
			System.exit(1);
		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
