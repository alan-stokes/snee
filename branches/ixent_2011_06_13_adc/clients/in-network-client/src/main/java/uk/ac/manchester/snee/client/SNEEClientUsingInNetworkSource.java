package uk.ac.manchester.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;

public class SNEEClientUsingInNetworkSource extends SNEEClient {

	public SNEEClientUsingInNetworkSource(String query, 
			double duration, String queryParams, String csvFile) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams, csvFile);
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
		Double duration;
		String queryParams;
		String csvFile=null;
		if (args.length != 3 && args.length!=4) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds ('inf' for indefinite)\"\n" +
					"\t\"query parameters file\"\n" +
					"\t[\"csv file to log results\"]\n");
			//XXX: Use default query
			query = "SELECT * FROM SeaDefence;";
//			query = "SELECT seaLevel, \'East\' FROM SeaDefence;";
			//query = "SELECT avg(seaLevel) FROM SeaDefence;";
			//query = "SELECT e.seaLevel FROM SeaDefenceEast[NOW] e, SeaDefenceWest[NOW] w WHERE e.seaLevel > w.seaLevel;";
			duration = Double.valueOf("120");
			queryParams= "etc/query-parameters.xml";
		} else {	
			query = args[0];
			if (args[1].equalsIgnoreCase("inf") || args[1].equalsIgnoreCase("ind")) {
				duration = Double.POSITIVE_INFINITY;
			} else {
				duration = Double.valueOf(args[1]);
			}
			queryParams = args[2];
			if (args.length==4) {
				csvFile=args[3];
			}
		}
			
		try {
			/* Initialise SNEEClient */
			SNEEClientUsingInNetworkSource client = 
				new SNEEClientUsingInNetworkSource(query, duration, queryParams, csvFile);
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
