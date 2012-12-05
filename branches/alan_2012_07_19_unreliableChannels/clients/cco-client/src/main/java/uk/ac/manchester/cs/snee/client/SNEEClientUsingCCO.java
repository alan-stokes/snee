package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientUsingCCO extends SNEEClient {
	
	private static final Logger logger = Logger.getLogger(SNEEClientUsingCCO.class.getName());
	
	private String serviceUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl";
//		"http://webgis1.geodata.soton.ac.uk:8080/EMU/services/PullStream?wsdl";
	
	private static String query =
//		"SELECT * FROM envdata_hernebay_tide;";
		"SELECT * FROM envdata_hernebay_met;";

//		"SELECT * FROM rtdata_haylingisl;";

//		"SELECT \'HerneBay\', timestamp, datetime, observed, " +
//			"tz, hs, hmax, tp " +
//			"FROM envdata_hernebay_tide;";


	public SNEEClientUsingCCO(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingCCO()");
		getController().addServiceSource("CCO-WS", serviceUrl, 
				SourceType.PULL_STREAM_SERVICE);
//		displayExtentNames();
//		displayAllExtents();
//		displayExtentSchema("envdata_haylingisland");
//		displayExtentSchema("envdata_teignmouthpier_tide");
//		displayExtentSchema("envdata_hernebay_met");
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
				SNEEClientUsingCCO.class.getClassLoader().
				getResource("etc/log4j.properties"));
		Long duration;
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
//			System.exit(1);
			//XXX: Use default settings
			duration = Long.valueOf("900");
		} else {	
			query = args[0];
			duration = Long.valueOf(args[1]);
		}
			
			try {
				/* Initialise and run SNEEClient */
				SNEEClientUsingCCO client = 
					new SNEEClientUsingCCO(query, duration);
				client.run();
				/* Stop the data source */
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
