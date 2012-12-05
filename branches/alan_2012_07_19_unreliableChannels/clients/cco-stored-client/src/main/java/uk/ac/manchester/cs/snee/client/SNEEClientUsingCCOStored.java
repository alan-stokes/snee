package uk.ac.manchester.cs.snee.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientUsingCCOStored extends SNEEClient 
{
	
  private static final Logger logger = Logger.getLogger(SNEEClientUsingCCOStored.class.getName());
	
	private String serviceUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/dai/services/";
		//XXX Bowfell only accessibly within the cs.man network (not VPN)
//		"http://bowfell.cs.man.ac.uk:8080/dai/services/";
	
	private static String extent = 
		"locations";
//		"metadata";
		
	private static String query =
		"SELECT * FROM locations;";
//		"SELECT id, location, latitude, longitude, waves, tides, met, storm_threshold FROM locations;";
//		"SELECT * FROM locations l WHERE l.id = 68;";
//		"SELECT * FROM locations l WHERE l.location = \'Folkestone\';";
//		"SELECT * FROM locations[RESCAN 20 SECONDS];";

	private static Long duration = Long.valueOf("30");
	
	public SNEEClientUsingCCOStored(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingCCOStored()");
		getController().addServiceSource("CCO-Stored", serviceUrl, 
				SourceType.WSDAIR);
//		Collection<String> extents = controller.getExtents();
//		Iterator<String> it = extents.iterator();
//		System.out.println("Extents:");
//		while (it.hasNext()) {
//			System.out.print("\t" + it.next() + "\n");
//		}
		displayExtentSchema(extent);
		displayExtentSchema("envdata_swanagepier_tide");
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
				SNEEClientUsingCCOStored.class.getClassLoader().
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
				/* Initialise and run SNEEClient */
				SNEEClientUsingCCOStored client = 
					new SNEEClientUsingCCOStored(query, duration);
				client.run();
				/* Stop the data source */
			} catch (Exception e) {
				System.out.println("Execution failed. See logs for detail. " + 
						e.getLocalizedMessage());
				logger.fatal(e);
				System.exit(1);
			}
//		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
