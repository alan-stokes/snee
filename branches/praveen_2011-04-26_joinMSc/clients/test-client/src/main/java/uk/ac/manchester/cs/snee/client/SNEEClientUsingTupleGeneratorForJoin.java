package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientUsingTupleGeneratorForJoin extends SNEEClient {
	
	private static ConstantRatePushStreamGenerator _myDataSource;
	
	private String ccoStoredUrl = 
		"http://localhost:3306/test";

	public SNEEClientUsingTupleGeneratorForJoin(String query, 
			double duration, String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException, 
	MetadataException, SNEEDataSourceException {
		super(query, duration,queryParams);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClietnUsingTupleGeneratorSource()");	
		/*controller.addServiceSource("CCO-Stored", ccoStoredUrl, 
				SourceType.RELATIONAL);*/
		displayAllExtents();
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
				SNEEClientUsingTupleGeneratorForJoin.class.
				getClassLoader().getResource("etc/log4j.properties"));
		Long duration;
		String query;
		String queryParams;
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
			//XXX: Use default query
			//query = "SELECT p.intattr, h.Tz, p.decattr, h.tp FROM PushStream[NOW] p, HerneBay_Tide[NOW] h " +
			//"WHERE p.intattr = h.Tz;";
			query = "SELECT p.intattr, h.Tz, p.decattr, h.tp " +
					"FROM PushStream[NOW] p, HerneBay_Tide[NOW] h, " +
					"HerneBay_Met[NOW] m " +
					"WHERE p.intattr = h.Tz AND p.intattr <= 42 AND " +
					"m.TAir <= h.Tz;";
			//"WHERE p.intattr = h.Tz AND p.decattr <= h.tp ;";
			//query = "SELECT p.intattr, h.Tz, p.timestamp, h.timestamp FROM PushStream[NOW] p, HerneBay_Tide[NOW] h " +
				//	"WHERE p.intattr = h.Tz AND p.timestamp <= h.timestamp;";

			//query = "SELECT * FROM PushStream;";
			
//			query = "SELECT intattr, intattr * 2 FROM PushStream;";
//			query = "SELECT \'const\', intattr FROM PushStream;";
//			query = "SELECT 42, intattr FROM PushStream;";
//			query = "SELECT \'const\' as StringConstant, intattr FROM PushStream;";

//			query = "SELECT * FROM PushStream WHERE stream_name = \'pushstream\';";
			
			/* The following queries should run */
			//SELECT-PROJECT
//			query = "SELECT intattr " +
//				"FROM PushStream " +
//				"WHERE intattr <= 5;";

			//SELECT-PROJECT-ALIAS
//			query = "SELECT p.intattr " +
//				"FROM PushStream p " +
//				"WHERE p.intattr <= 5;";

			//SELECT-PROJECT-ALIAS-RENAME
//			query = "SELECT p.intattr AS IntegerValue " +
//				"FROM PushStream p " +
//				"WHERE p.intattr <= 5;";

			//JOIN
//			query = "SELECT * " +
//					"FROM PushStream[NOW] p, HerneBay_Tide[NOW] h " +
//					"WHERE p.intattr <= h.Tz;";

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
			queryParams= "etc/query-parameters.xml";
//			System.exit(1);
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
			queryParams = args[2];
		}
			
			try {
				/* Initialise SNEEClient */
				SNEEClientUsingTupleGeneratorForJoin client = 
					new SNEEClientUsingTupleGeneratorForJoin(query, duration, queryParams);
				
				
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
