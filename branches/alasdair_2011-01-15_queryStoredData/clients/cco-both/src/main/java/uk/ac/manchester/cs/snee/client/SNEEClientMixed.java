package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientMixed extends SNEEClient {
	
	private static Logger logger = 
		Logger.getLogger(SNEEClientMixed.class.getName());
	
//	private static ConstantRatePushStreamGenerator _myDataSource;
	
	private String ccoWsUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl";
	private String ccoStoredUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/dai/services/";

	private static String query =
		"SELECT l.location, l.storm_threshold, t.Hs, t.HMax " +
		"FROM envdata_swanagepier_tide" +
		"[FROM NOW - 10 MINUTES TO NOW SLIDE 1 MINUTE] t, " +
			"locations l " +
		"WHERE  " + 
		       "(t.Hs <= l.storm_threshold OR " +
		        "t.HMax <= l.storm_threshold);";
		
//		"SELECT l.location, l.storm_threshold, t.Hs, t.HMax " +
//		"FROM envdata_swanagepier_tide t, " +
//			"locations l " +
//		"WHERE  " + 
//		       "(t.Hs <= l.storm_threshold OR " +
//		        "t.HMax <= l.storm_threshold);";
		
//		"SELECT l.location, l.storm_threshold, t.Hs, t.HMax " +
//		"FROM envdata_swanagepier_tide" +
//		"[FROM NOW - 10 MINUTES TO NOW SLIDE 1 MINUTE] t, " +
//			"locations[RESCAN 15 MINUTES] l " +
//		"WHERE  " + 
//		       "(t.Hs <= l.storm_threshold OR " +
//		        "t.HMax <= l.storm_threshold);";

//		"SELECT location, Hs, HMax " +
//		"FROM envdata_hernebay_tide[FROM NOW - 1 MINUTE TO NOW SLIDE 1 MINUTE], " +
//			"locations " +
//		"WHERE  " +//w.location = l.locaction AND " + 
//	       "(Hs >= storm_threshold OR " +
//	        "HMax >= storm_threshold);";
	
	private static long duration = 900;
//	private static long duration = 10;
	
	public SNEEClientMixed(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientMixed()");

		//XXX: Order of adding services is important!
		controller.addServiceSource("CCO-WS", ccoWsUrl, 
				SourceType.PULL_STREAM_SERVICE);
		controller.addServiceSource("CCO-Stored", ccoStoredUrl, 
				SourceType.WSDAIR);
//		displayExtentNames();
//		displayAllExtents();
		displayExtentSchema("locations");
		displayExtentSchema("envdata_swanagepier_tide");
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
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
				SNEEClientMixed.class.getClassLoader().
				getResource("etc/log4j.properties"));
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
			//XXX: Use default settings
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
		}			
		try {
			/* Initialise SNEEClient */
			SNEEClientMixed client = 
				new SNEEClientMixed(query, duration);
			/* Initialise and run data source */
//			_myDataSource = new ConstantRatePushStreamGenerator();
//			_myDataSource.startTransmission();
			/* Run the client */
			client.run();
			/* Stop the data source */
//			_myDataSource.stopTransmission();
		} catch (Exception e) {
			System.out.println("Execution failed. See logs for detail.");
			logger.fatal(e);
			System.exit(1);
		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
