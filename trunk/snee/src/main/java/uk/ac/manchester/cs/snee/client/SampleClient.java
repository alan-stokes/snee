package uk.ac.manchester.cs.snee.client;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;

public class SampleClient implements Observer {
	
	private static Logger logger = 
		Logger.getLogger(SampleClient.class.getName());
	
	private String _query;
	private double _duration;
	private String _queryParams;

	private SNEEController controller;
	
	public SampleClient(String propertiesFile, String query, double duration, 
						String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SampleClient()");
		_query = query;
		_duration = duration;
		_queryParams = queryParams;
		controller = new SNEEController(propertiesFile);		
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

	public void displayExtents()
	throws MetadataException
	{
		Collection<String> extents = controller.getExtents();
		for (String extent : extents) {
			displayExtentSchema(extent);
		}
	}
	
	private void displayExtentSchema(String extentName) 
	throws MetadataException 
	{
		ExtentMetadata extent = 
			controller.getExtentDetails(extentName);
		List<Attribute> attributes = extent.getAttributes();
		System.out.println("Attributes for " + extentName + ":");
		for (Attribute attr : attributes) {
			String attrName = attr.getAttributeDisplayName();
			AttributeType attrType = attr.getType();
			System.out.print("\t" + attrName + ": " + 
					attrType.getName() + "\n");
		}
		System.out.println();
	}
	
	private void printResults(List<ResultSet> results, 
									 int queryId) 
	throws SQLException {
		System.out.println("************ Results for query " + 
						   queryId + " ************");
		for (ResultSet rs : results) {
			ResultSetMetaData metaData = rs.getMetaData();
			int numCols = metaData.getColumnCount();
			printColumnHeadings(metaData, numCols);
			while (rs.next()) {
				StringBuffer buffer = new StringBuffer();
				for (int i = 1; i <= numCols; i++) {
					Object value = rs.getObject(i);
					if (metaData.getColumnType(i) == 
						Types.TIMESTAMP && value instanceof Long) {
						buffer.append(
									  new Date(((Long) value).longValue()));
					} else {
						buffer.append(value);
					}
					buffer.append("\t");
				}
				System.out.println(buffer.toString());
			}
		}
		System.out.println("*********************************");
	}
	
	private void printColumnHeadings(ResultSetMetaData metaData,
											int numCols) throws SQLException {
		StringBuffer buffer = new StringBuffer();
		for (int i = 1; i <= numCols; i++) {
			buffer.append(metaData.getColumnLabel(i));
			//			buffer.append(":" + metaData.getColumnTypeName(i));
			buffer.append("\t");
		}
		System.out.println(buffer.toString());
	}
	
	public void update (Observable observation, Object arg) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() with " + observation + " " + 
						 arg);
		}
		//		logger.trace("arg type: " + arg.getClass());
		if (arg instanceof List<?>) {
			List<ResultSet> results = (List<ResultSet>) arg; 
			try {
				printResults(results, 1);
			} catch (SQLException e) {
				logger.error("Problem printing result set. ", e);
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}
	
	public void run() 
	throws SNEECompilerException, MetadataException, EvaluatorException,
	SNEEException, SQLException, SNEEConfigurationException {
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER");
		System.out.println("Query: " + this._query);
		
		int queryId1 = controller.addQuery(_query, _queryParams);
		
		long startTime = System.currentTimeMillis();
		long endTime = (long) (startTime + (_duration * 1000));
		
		System.out.println("Running query for " + _duration + 
						   " seconds. Scheduled end time " + new Date(endTime));
		
		ResultStoreImpl resultStore = 
		(ResultStoreImpl) controller.getResultStore(queryId1);
		resultStore.addObserver(this);
		
		try {			
			Thread.currentThread().sleep((long)_duration * 1000);
		} catch (InterruptedException e) {
		}
		
		while (System.currentTimeMillis() < endTime) {
			Thread.currentThread().yield();
		}
		
		List<ResultSet> results1 = resultStore.getResults();
		System.out.println("Stopping query " + queryId1 + ".");
		controller.removeQuery(queryId1);
		
		controller.close();
		printResults(results1, queryId1);
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}
	
	/**
	 * The main entry point for the Sample Client
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		// Configure logging
		PropertyConfigurator.configure(
				SampleClient.class.getClassLoader().
				getResource("etc/log4j.properties"));
		String propertiesFile = null;
		String query = null;
		Long duration = null;
		String params = null;
		//This method represents the web server wrapper
		if (args.length < 3 || args.length > 4) {
			System.out.println("Usage: \n" +
					"\t\"location of SNEE properties file\"\n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds\"\n" +
					"\t\"optional third argument stating location of query parameters file\"");
			System.exit(1);
		} else {
			propertiesFile = args[0];
			query = args[1];
			duration = Long.valueOf(args[2]);
			if (args.length == 4) {
				params = args[3];
			}
		}
			
		try {
			/* Initialise the Client */
			SampleClient client = 
				new SampleClient(propertiesFile, query, duration, params);
			/* Print the available extents */
			client.displayExtents();
			/* Run the client */
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
