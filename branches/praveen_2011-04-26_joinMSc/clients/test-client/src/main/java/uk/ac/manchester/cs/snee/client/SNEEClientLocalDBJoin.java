package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientLocalDBJoin extends SNEEClient {
	
	private static Logger logger = 
		Logger.getLogger(SNEEClientLocalDBJoin.class.getName());
	
//	private static ConstantRatePushStreamGenerator _myDataSource;
	
	
	private String ccoLocalDBUrl = 
		"jdbc:mysql://localhost:3306/cco?user=cco&password=cco";
	
	
	public SNEEClientLocalDBJoin(String query, double duration,  String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException, SchemaMetadataException 
	{
		super(query, duration, queryParams);
		
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientMixedP()");

		//XXX: Order of adding services is important!
		
		controller.addServiceSource("test-db", ccoLocalDBUrl, 
				SourceType.RELATIONAL);
//		displayExtentNames();
		displayAllExtents();
		//displayExtentSchema("locations");
		//displayExtentSchema("envdata_swanagepier_tide");
		//displayExtentSchema("envdata_hernebay_tide");
		//displayExtentSchema("SeaDefenceEast");
		//displayExtentNames();
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}	
	
	/**
	 * The main entry point for the SNEE controller
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) {
		// Configure logging
		PropertyConfigurator
				.configure(SNEEClientUsingTupleGeneratorForJoin.class
						.getClassLoader().getResource("etc/log4j.properties"));
		Long duration;
		String query;
		String queryParams;
		// This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" + "\t\"query statement\"\n"
					+ "\t\"query duration in seconds\"\n");
			
			query =
				 "RSTREAM SELECT p.timestamp, p.intattr, h.intattr, p.decattr, h.doubleattr FROM PushStream[NOW] p, LOCAL_TUPLE_TABLE h "+			
				"WHERE p.intattr = h.intattr;";			
			
			/*query =
				 "RSTREAM SELECT p.timestamp, p.intattr, h.intattr, p.decattr, h.doubleattr, t.Tz FROM PushStream[NOW] p, HerneBay_Tide[NOW] t, LOCAL_TUPLE_TABLE h "+			
				"WHERE p.intattr = h.intattr and p.intattr = t.Tz;";*/
			
			
			duration = Long.valueOf("20");
			queryParams = "etc/query-parameters.xml";
			// System.exit(1);
		} else {
			query = args[0];
			duration = Long.valueOf(args[1]);
			queryParams = args[2];
		}

		try {
			/* Initialise SNEEClient */
			SNEEClientLocalDBJoin client = new SNEEClientLocalDBJoin(
					query, duration, queryParams);

			/* Initialise and run data source */
			//_myDataSource = new ConstantRatePushStreamGenerator();
			//_myDataSource.startTransmission();
			/* Run SNEEClient */
			client.run();
			/* Stop the data source */
			//_myDataSource.stopTransmission();
			
			System.out.println("Came here");
		} catch (Exception e) {
			System.out.println("Execution failed. See logs for detail.");
			logger.fatal("Execution failed", e);
			System.exit(1);
		}
		// }
		System.out.println("Success!");
		System.exit(0);
	}
	
}
