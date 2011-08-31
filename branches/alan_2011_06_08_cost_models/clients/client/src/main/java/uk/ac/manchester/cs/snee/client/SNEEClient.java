package uk.ac.manchester.cs.snee.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEE;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlanAbstract;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;

public abstract class SNEEClient implements Observer {

	protected static Logger logger = 
		Logger.getLogger(SNEEClient.class.getName());
	protected SNEE controller;
	protected String _query;
	protected double _duration;
	protected String _queryParams;
	private String _csvFilename;

	private boolean firstTime = true;
	private boolean keepOn = true;
	public boolean displayResultsAtEnd = false;

	
	public SNEEClient(String query, double duration, String queryParams, 
			String csvFilename) 
	throws SNEEException, IOException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEClient() with query " + query + 
					" duration " + duration);
		
		_query = query;
		_duration = duration;
		_queryParams = queryParams;
		controller = new SNEEController("etc/snee.properties");
		_csvFilename = csvFilename;

		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClient()");
	}
	
    public SNEEClient(String query, double duration, String queryParams, 
    		          String csvFilename,  String sneeProperties)
    throws SNEEException, IOException, SNEEConfigurationException {
      if (logger.isDebugEnabled())
        logger.debug("ENTER SNEEClient() with query " + query + 
            " duration " + duration);
      
      _query = query;
      _duration = duration;
      _queryParams = queryParams;
      _csvFilename = csvFilename;
      controller = new SNEEController(sneeProperties);

      if (logger.isDebugEnabled())
        logger.debug("RETURN SNEEClient()");
    }

	
	public SNEEClient(String query, double duration, String csvFilename) 
	throws SNEEException, IOException, SNEEConfigurationException {
		this(query, duration, null, csvFilename);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEClient() with query " + query + 
					" duration " + duration);
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClient()");
	}

	public SNEEClient(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException {
		this(query, duration, null, null);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEClient() with query " + query + 
					" duration " + duration);
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClient()");
	}
	
	protected void displayExtentNames() {
		Collection<String> extentNames = controller.getExtentNames();
		Iterator<String> it = extentNames.iterator();
		System.out.println("Extents:");
		while (it.hasNext()) {
			System.out.print("\t" + it.next() + "\n");
		}

	}
	
	protected void displayAllExtents() throws MetadataException {
		Collection<String> extents = controller.getExtentNames();
		Iterator<String> it = extents.iterator();
		while (it.hasNext()) {
			String extentName = it.next();
			displayExtentSchema(extentName);
		}
	}
	
	protected void displayExtentSchema(String extentName) 
	throws MetadataException 
	{
		ExtentMetadata extent = 
			controller.getExtentDetails(extentName);
		List<Attribute> attributes = extent.getAttributes();
		System.out.println("Attributes for " + extentName + " [" + 
				extent.getExtentType() + "]" + ":");
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
	
	public void run() 
	throws SNEECompilerException, MetadataException, EvaluatorException,
	SNEEException, SQLException, SNEEConfigurationException, FileNotFoundException {
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER");
		

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
			logger.debug("RETURN");
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
		
		System.err.println("You should never see this");

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
			System.out.println("Success!");
        }
    }
	public SensorNetworkQueryPlan getQEP()
	{
	  SNEEController control = (SNEEController) controller;
	  return (SensorNetworkQueryPlan) control.getQEP();
	}
	
	public void setDeadNodes(ArrayList<String> deadNodes)
  {
	  SNEEController control = (SNEEController) controller;
    control.setDeadNodes(deadNodes);
  }
  
  public void setDeadNodes(int noDeadNodes)
  {
    SNEEController control = (SNEEController) controller;
    control.setNoDeadNodes(noDeadNodes);
  }
	
}