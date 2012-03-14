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

			
			/********************************************************************
			 * 				QUERIES USING DATA ANALYSIS TECHNIQUES				*
			 ********************************************************************/

			query = "RSTREAM SELECT sd.seaLevel, lr.voltage " +
			"FROM SeaDefenceLRF lr, SeaDefence[NOW] sd " +
			"WHERE sd.seaLevel=lr.seaLevel;";

//			query = "SELECT RSTREAM od.seaLevel " +
//			"FROM ( " +
//			"  SELECT RSTREAM q5.seaLevel, (1/q5.cnt)*((3/4)^1)*(1/(q5.scotts1))*q5.q as probability " +
//			"  FROM ( " +
//			"    SELECT RSTREAM x.seaLevel, 2*5-(1/(3*(q4.scotts1^2)))*((q4.diff1+5)^3 - (q4.diff1-5)^3) as q, q4.cnt as cnt" +
//			"    FROM ( " +
//			"      SELECT x.seaLevel, ( x.seaLevel - y.seaLevel ) as diff1, ( x.seaLevel - y.seaLevel ) / q41.scotts1 as absVal1, q41.scotts1, q41.cnt" +
//			"      FROM ( " +
//			"        SELECT RSTREAM SQRT(5) * q2.sigma1 * (q3.rsize^(- 1/(4+1))) as scotts1, q2.cnt as cnt" +
//			"        FROM ( " +
//			"          SELECT RSTREAM SQRT( q1.sqrSD1 ) as sigma1, q1.cnt" +
//			"          FROM ( " +
//			"            SELECT RSTREAM SUM(q11.sqr_x1) / COUNT(q11.sqr_x1) as sqrSD1, COUNT(q11.sqr_x1) as cnt" +
//			"            FROM ( " +
//			"              SELECT (t.seaLevel - src.avg1)^2 as sqr_x1" +
//			"              FROM " +
//			"                seaDefence[FROM NOW-20 MIN TO NOW SLIDE 5 MIN] t," +
//			"                ( " +
//			"                  SELECT RSTREAM AVG( seaLevel ) as avg1 " +
//			"                  FROM seaDefence[FROM NOW-20 MIN TO NOW SLIDE 5 MIN] " +
//			"                ) src" +
//			"              ) q11" +
//			"            ) q1" +
//			"          ) q2," +
//			"          (" +
//			"            SELECT RSTREAM COUNT(*) as rsize" +
//			"            FROM seaDefenceEast[FROM NOW-10 MIN TO NOW SLIDE 10 MIN]" +
//			"          ) q3" +
//			"        ) q41," +
//			"        seaDefence[now] x," +
//			"        seaDefenceEast[FROM NOW-10 MIN TO NOW SLIDE 10 MIN] y" +
//			"      ) q4" +
//			"      WHERE abs( q4.absVal1 ) < 1" +
//			"    ) q5 " +
//			"  ) od," +
//			"  seaDefence[now] sdSrc " +
//			"WHERE od.probability < 0.15 AND od.seaLevel=sdSrc.seaLevel";

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
		System.exit(0);
	}
	
}
