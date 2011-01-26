package uk.ac.manchester.cs.snee.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

public class SNEEClientUsingCCOStored extends SNEEClient {
	
	private static Logger logger = 
		Logger.getLogger(SNEEClientUsingCCOStored.class.getName());
	
	private String serviceUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/dai/services/";
//		"http://bowfell.cs.man.ac.uk:8080/dai/services/";

	public SNEEClientUsingCCOStored(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingCCOStored()");
		//Set sleep to 10 minutes
		_sleepDuration = 600000;
		controller.addServiceSource("CCO-Stored", serviceUrl, 
				SourceType.WSDAIR);
//		Collection<String> extents = controller.getExtents();
//		Iterator<String> it = extents.iterator();
//		System.out.println("Extents:");
//		while (it.hasNext()) {
//			System.out.print("\t" + it.next() + "\n");
//		}
		displayExtentSchema("locations");
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
		String query;
		Long duration;
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
//			System.exit(1);
			//XXX: Use default settings
			query = "SELECT id, location, latitude, longitude, waves, tides, met " +
					"FROM locations;";
			duration = Long.valueOf("900");
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
				System.out.println("Execution failed. See logs for detail.");
				logger.fatal(e);
				System.exit(1);
			}
//		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
