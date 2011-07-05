package uk.ac.manchester.cs.snee.client;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
	
	private SNEEController controller;
	protected String _propertiesFile;
	protected String _query = null;
	protected double _duration;
	protected String _queryParams = null;
	private String _csvFilename = null;
	
	private boolean firstTime = true;
	private boolean keepOn = true;
	public boolean displayResultsAtEnd = false;
	
	/**
	 * Configures the SNEE query engine according to the properties
	 * specified in the properties file.
	 * 
	 * @param propertiesFile location of the snee configuration file
	 * @throws SNEEException
	 * @throws IOException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws SNEEDataSourceException 
	 */
	public SampleClient(String propertiesFile, String query, double duration, 
						String queryParameters, String csvFilename)
	throws SNEEException, IOException, SNEEConfigurationException,
	MetadataException, SNEEDataSourceException 
	{
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SampleClient()");
		controller = new SNEEController(propertiesFile);
		this._propertiesFile = propertiesFile;
		this._query = query;
		this._duration = duration;
		this._queryParams = queryParameters;
		this._csvFilename = csvFilename;
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}
	
	/**
	 * Displays the extents available for querying
	 */
	public void displayExtents()
	throws MetadataException
	{
		Collection<String> extents = controller.getExtentNames();
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
	
	private void printRow(ResultSet rs, ResultSetMetaData metaData,
			int numCols, String sep, PrintStream out) throws SQLException {
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
			buffer.append(sep);
		}
		out.println(buffer.toString());
	}
	
	private void printResults(List<ResultSet> results, 
			int queryId, String csvFilename) 
	throws SQLException, FileNotFoundException {
    	PrintStream out = null;
    	if (csvFilename != null)
    		out = new PrintStream(new FileOutputStream(csvFilename, true));
		
		System.out.println("************ Results for query " + 
				queryId + " ************");
		for (ResultSet rs : results) {
			ResultSetMetaData metaData = rs.getMetaData();
			int numCols = metaData.getColumnCount();
			if (firstTime && csvFilename != null) {
				printColumnHeadings(metaData, numCols, ",", out);
				firstTime = false;
			}
			
			printColumnHeadings(metaData, numCols, "\t", System.out);
			while (rs.next()) {
				printRow(rs, metaData, numCols, "\t", System.out);
				if (csvFilename != null)
					printRow(rs, metaData, numCols, ",", out);
			}
		}
		System.out.println("*********************************");
	}
	
	private void printColumnHeadings(ResultSetMetaData metaData,
			int numCols, String sep, PrintStream out) throws SQLException {
		StringBuffer buffer = new StringBuffer();
		for (int i = 1; i <= numCols; i++) {
			buffer.append(metaData.getColumnLabel(i));
//			buffer.append(":" + metaData.getColumnTypeName(i));
			buffer.append(sep);
		}
		out.println(buffer.toString());
	}

	
	public void update (Observable observation, Object arg) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() with " + observation + " " + 
					arg);
		}
		if (arg instanceof List<?>) {
			List<ResultSet> results = (List<ResultSet>) arg; 
			try {
				printResults(results, 1, _csvFilename);
			} catch (SQLException e) {
				logger.error("Problem printing result set. ", e);
			} catch (FileNotFoundException e) {
				logger.error("Problem writing results to csv file. ", e);				
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}
	
	
	/**
	 * Execute a SNEEql query using the configured SNEE query execution
	 * engine.
	 * 
	 * @param query SNEEql query string to be executed
	 * @param duration length in seconds to execute the query 
	 * @param queryParams location of the parameters file associated with the query
	 */
	public void executeQuery() 
	throws SNEECompilerException, MetadataException, EvaluatorException,
	SNEEException, SQLException, SNEEConfigurationException {
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER executeQuery() with query " + _query +
					" parameters " + _queryParams + 
					" duration " + _duration);
		
		System.out.println("Query: " + this._query);

		int queryId1 = controller.addQuery(_query, _queryParams);

		long startTime = System.currentTimeMillis();
		ResultStoreImpl resultStore = 
			(ResultStoreImpl) controller.getResultStore(queryId1);
		resultStore.addObserver(this);

		Runtime.getRuntime().addShutdownHook(new RunWhenShuttingDown(queryId1, 
				resultStore));		
		if (_duration == Double.POSITIVE_INFINITY) {
			runQueryIndefinitely();
		} else {
			runQueryForFixedPeriod(startTime);
		}
		displayResultsAtEnd = true;
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN executeQuery()");
	}
	
	private void runQueryIndefinitely() throws SNEEException {
		System.out.println("Running query indefinitely. Press CTRL+C to exit.");

		while (keepOn) {
			try {
				Thread.currentThread().sleep(10000);
			} catch (InterruptedException e) {
			}
			Thread.currentThread().yield();
		}		
	}

	private void runQueryForFixedPeriod(long startTime) 
	throws SNEEException {
		long endTime = (long) (startTime + (_duration * 1000));
		System.out.println("Running query for " + _duration + " seconds. Scheduled end time " + new Date(endTime));
		
		try {			
			Thread.currentThread().sleep((long)_duration * 1000);
		} catch (InterruptedException e) {
		}
		
		while (System.currentTimeMillis() < endTime) {
			Thread.currentThread().yield();
		}
	}

	public class RunWhenShuttingDown extends Thread {
        private int _queryId;
        private ResultStoreImpl _resultStore;
        
        public RunWhenShuttingDown(int queryId, ResultStoreImpl resultStore) {
        	_queryId = queryId;
        	_resultStore = resultStore;
        }
        
		public void run() {
            keepOn = false;
    		System.out.println("Stopping query " + _queryId + ".");
    		try {
    			List<ResultSet> results1 = _resultStore.getResults();
				controller.removeQuery(_queryId);

				//XXX: Sleep included to highlight evaluator not ending bug 
				//Thread.currentThread().sleep((long) ((_duration/2) * 1000));
				Thread.sleep(2000);
				
				controller.close();				
				if (displayResultsAtEnd)
					printResults(results1, _queryId, null);
    		
    		} catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SNEEException e1) {
				e1.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
    }
	
	/**
	 * The main entry point for the Sample Client
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		String propertiesFile = null;
		String query = null;
		Double duration = null;
		String params = null;
		String csvFile = null;
		
		// Configure logging
		PropertyConfigurator.configure(
				SampleClient.class.getClassLoader().
				getResource("etc/log4j.properties"));
		//This method represents the web server wrapper
		if (args.length != 5) {
			System.out.println("Usage: \n" +
					"\t\"location of SNEE properties file\"\n" +
					"\t\"query statement\"\n" +
					"\t\"query duration in seconds, or 'ind' for indefinite\"\n" +
					"\t\"location of query parameters file, or null\"" +
					"\t\"csv file to log results, or null\"\n");
			System.exit(1);
		} else {
			propertiesFile = args[0];
			query = args[1];
			if (args[2].equalsIgnoreCase("inf") || args[2].equalsIgnoreCase("ind")) {
				duration = Double.POSITIVE_INFINITY;
			} else {
				duration = Double.valueOf(args[2]);
			}
			if (!(args[3].equalsIgnoreCase("null"))) {
				params = args[3];
			}
			if (!(args[4].equalsIgnoreCase("null"))) {
				csvFile = args[4];
			}
		}
			
		try {
			/* Initialise the Client */
			SampleClient client = 
				new SampleClient(propertiesFile, query, duration, params, csvFile);
			/* Print the available extents */
			client.displayExtents();
			/* Execute the query */
			client.executeQuery();
		} catch (Exception e) {
			System.out.println("Execution failed. See logs for detail.");
			logger.fatal(e);
			System.exit(1);
		}

		System.out.println("Success!");
		System.exit(0);
	}
	
}
