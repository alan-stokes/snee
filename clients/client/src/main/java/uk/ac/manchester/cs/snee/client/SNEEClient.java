package uk.ac.manchester.cs.snee.client;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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

	public SNEEClient(String query, double duration, String queryParams) 
	throws SNEEException, IOException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEClient() with query " + query + 
					" duration " + duration);
		
		_query = query;
		_duration = duration;
		_queryParams = queryParams;
		controller = new SNEEController("etc/snee.properties");

		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEClient()");
	}
	
	public SNEEClient(String query, double duration) 
	throws SNEEException, IOException, SNEEConfigurationException {
		this(query, duration, null);
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

		//		try {
		int queryId1 = controller.addQuery(_query, _queryParams);
		//		int queryId2 = controller.addQuery(query);

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

		try {
			//XXX: Sleep included to highlight evaluator not ending bug 
			Thread.currentThread().sleep((long) ((_duration/2) * 1000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		controller.close();
		printResults(results1, queryId1);
		//		printResults(results2, queryId2);
		if (logger.isDebugEnabled())
			logger.debug("RETURN");
	}

}