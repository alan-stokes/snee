package uk.ac.manchester.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.Utils;
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
				getClassLoader().getResource("etc/common/log4j.properties"));
		String query;
		Long duration;
		String queryParams;
		if (args.length != 3) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n" +
					"\t\"query parameters file\"\n");
			//XXX: Use default query
			
			query = "SELECT RSTREAM avg(c.light) " +
			//query = "SELECT *" +
			//query = "SELECT * " +
			//query = "SELECT RSTREAM c.light, m.light " +
		  //query = "SELECT c.light, m.light " +
		  //query = "SELECT c.light, m.light, f.light " +
			
		      //"FROM Castilla[now] c, Meadow[now] m, Forest[now] f WHERE c.light < m.light AND m.light > f.light;";
					//"FROM Castilla[now] c, Meadow[now] m WHERE c.light < m.light;";
		    	//"FROM Castilla[now] c, Forest[now] f WHERE c.light < f.light;";
			    //"FROM Castilla[RANGE 10 ROWS SLIDE 10 ROWS] c;";
			    //"FROM Castilla[FROM NOW - 5 ROWS TO NOW SLIDE 5 ROWS] c;";
			    //"FROM Castilla[FROM NOW - 5 TO NOW MINUTES] c;";
			    "FROM Castilla[now] c;";
			    //"FROM (SELECT avg(m.light) as mLight FROM Meadow[now] m) a, " +
			    //"(SELECT avg(f.light) as fLight FROM Forest[now] f) b " +
			    //"WHERE a.mLight = b.fLight;";
			duration = Long.valueOf("120");
			queryParams= "etc/common/query-parameters.xml";
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
//				_myDataSource = new ConstantRatePushStreamGenerator();
//				_myDataSource.startTransmission();
				/* Run SNEEClient */
				client.run();
				/* Stop the data source */
//				_myDataSource.stopTransmission();
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
