package uk.ac.manchester.cs.snee.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;

public class SNEEClientUsingCCO extends SNEEClient {
	
	private static Logger logger = 
		Logger.getLogger(SNEEClientUsingCCO.class.getName());
	
	private String serviceUrl = 
		"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl";
	

	public SNEEClientUsingCCO(String query, double duration) 
	throws SNEEException, IOException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException, ExtentDoesNotExistException, SNEEConfigurationException {
		super(query, duration);
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEEClientUsingCCO()");
		//Set sleep to 10 minutes
		_sleepDuration = 600000;
		controller.addServiceSource(serviceUrl);
//		Collection<String> extents = controller.getExtents();
//		Iterator<String> it = extents.iterator();
//		System.out.println("Extents:");
//		while (it.hasNext()) {
//			System.out.print("\t" + it.next() + "\n");
//		}
//		displayExtentSchema("envdata_haylingisland");
//		displayExtentSchema("envdata_teignmouthpier_tide");
//		displayExtentSchema("envdata_hernebay_met");
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	private void displayExtentSchema(String extentName) 
	throws ExtentDoesNotExistException {
		Iterator<String> it;
		ExtentMetadata extent = 
			controller.getExtentDetails(extentName);
		Map<String, AttributeType> attributes = extent.getAttributes();
		Set<String> attrNames = attributes.keySet();
		it = attrNames.iterator();
		System.out.println("Attributes for " + extentName + ":");
		while (it.hasNext()) {
			String attrName = it.next();
			AttributeType attr = attributes.get(attrName);
			System.out.print("\t" + attrName + ": " + attr.getName() + "\n");
		}
		System.out.println();
	}
	
	/**
	 * The main entry point for the SNEE controller
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		//This method represents the web server wrapper
		if (args.length != 2) {
			System.out.println("Usage: \n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n");
			System.exit(1);
		} else {	
			// Configure logging
			PropertyConfigurator.configure("etc/log4j.properties");
			
			String query = args[0];
			long duration = Long.valueOf(args[1]);
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
		}
		System.out.println("Success!");
		System.exit(0);
	}
	
}
