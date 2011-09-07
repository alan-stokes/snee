package uk.ac.manchester.cs.snee.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClientUsingTupleGeneratorForJoin;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

/**
 * @author Praveen
 * 
 */
public class TestDBClient {

	private SNEEClientUsingTupleGeneratorForJoin clientUsingTupleGeneratorForJoin = null;
	private static final String ORIGINAL_FILE = "Original";
	private static final String OUTPUT_PATH = "exp_output";
	protected static Logger logger = Logger.getLogger(TestDBClient.class
			.getName());
	private static final String COLON_SEPARATOR = ":";
	private static final String DOUBLE_SLASH_SEPARATOR = "//";
	private static final String SINGLE_SLASH_SEPARATOR = "/";
	private static final String QUESTION_MARK = "?";

	public static void main(String[] args) throws ParserConfigurationException,
			SAXException {
		String url = "";
		try {

			PropertyConfigurator.configure(TestDBClient.class.getClassLoader()
					.getResource("etc/log4j.properties"));

			TestDBClient testClient = new TestDBClient();

			ConnectionDetails connectionDetails = testClient
					.getConnectionDetailsFromSystemFile();
			url = testClient.buildUrl(connectionDetails.getDbType(),
					connectionDetails.getHost(), connectionDetails.getPort(),
					connectionDetails.getDbschema(),
					connectionDetails.getDbUserName(),
					connectionDetails.getDbPassword());
			Collection<QueryParameters> queryParameters = testClient
					.readInput();
			if (queryParameters != null && queryParameters.size() > 0) {
				testClient.executeEachQuery(queryParameters, url);
			}
		} catch (URISyntaxException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} finally {
			System.exit(0);
		}

	}

	private void executeEachQuery(Collection<QueryParameters> queryParameters,
			String url) throws URISyntaxException, IOException {
		String prevQueryId = ORIGINAL_FILE;
		String outputPath = OUTPUT_PATH;
		File outputLocation = new File(outputPath);
		if (!outputLocation.exists()) {
			outputLocation.mkdir();
		}
		deleteContentsOfDir(outputLocation);

		String sourceLocation = this.getClass().getResource("etc").getFile();
		String destLocation;
		for (QueryParameters parameters : queryParameters) {
			destLocation = outputPath + "/" + prevQueryId;
			// Copying the current etc contents into a backup folder
			// dependent on what the previous experiment was
			copyContents(new File(sourceLocation), new File(destLocation));
			// Copying the contents from the experiment specific etc path
			// to the current etc path
			copyContents(new File(parameters.getQueryParamsPath()), new File(
					sourceLocation));
			prevQueryId = parameters.getQueryId();
			// Carry out the experiment
			executeQuery(parameters, url);
			// Copy the log to the same destination path for later analysis
			copyContents(new File("logs"), new File(sourceLocation));

		}
		// For the Last experiment
		destLocation = outputPath + "/" + prevQueryId;
		copyContents(new File(sourceLocation), new File(destLocation));
		// Copying the original contents back into the etc directory
		copyContents(new File(outputPath + "/" + ORIGINAL_FILE), new File(
				sourceLocation));
	}

	/**
	 * Execute the query
	 * 
	 * @param parameters
	 * @param url
	 */
	private void executeQuery(QueryParameters parameters, String url) {
		PropertyConfigurator.configure(TestDBClient.class.getClassLoader()
				.getResource("etc/log4j.properties"));
		try {
			logger.info("Starting Query " + parameters.getQuery()
					+ " with duration " + parameters.getDuration()
					+ " and experiment path " + parameters.getQueryParamsPath());
			clientUsingTupleGeneratorForJoin = new SNEEClientUsingTupleGeneratorForJoin(
					parameters.getQuery(), parameters.getDuration(),
					"etc/query-parameters.xml");
			clientUsingTupleGeneratorForJoin.addJDBCSource(url);
			clientUsingTupleGeneratorForJoin.run();

		} catch (SNEEException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SNEEConfigurationException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (MetadataException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SNEEDataSourceException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SNEECompilerException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (EvaluatorException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SQLException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SchemaMetadataException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} finally {
			clientUsingTupleGeneratorForJoin = null;
		}

	}

	private void copyContents(File sourceLocation, File targetLocation)
			throws IOException {
		System.out.println("Copying from " + sourceLocation.getAbsolutePath()
				+ " to " + targetLocation.getAbsolutePath());
		if (sourceLocation.isDirectory()) {
			if (!targetLocation.exists()) {
				targetLocation.mkdir();
			}
			File[] files = sourceLocation.listFiles();
			for (File file : files) {
				if (file.isFile()) {
					InputStream in = new FileInputStream(file);
					OutputStream out = new FileOutputStream(targetLocation
							+ "/" + file.getName());
					// Copy the bits from input stream to output stream
					byte[] buf = new byte[1024];
					int len;
					while ((len = in.read(buf)) > 0) {
						out.write(buf, 0, len);
					}
					in.close();
					out.close();
				}
			}
		} else if (sourceLocation.isFile()) {
			if (!targetLocation.exists()) {
				targetLocation.mkdir();
			}
			InputStream in = new FileInputStream(sourceLocation);
			OutputStream out = new FileOutputStream(targetLocation + "/"
					+ sourceLocation.getName());

			// Copy the bits from input stream to output stream
			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
		}

	}

	private Collection<QueryParameters> readInput() {
		System.out.println(this.getClass().getResource("Input.txt"));
		File file = null;
		Collection<QueryParameters> queryParameters = null;
		try {
			file = new File(this.getClass().getResource("Input.txt").toURI());
			queryParameters = getQueryParametersfromFile(file);
			System.out.println(queryParameters);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return queryParameters;
	}

	private Collection<QueryParameters> getQueryParametersfromFile(File file) {
		Collection<QueryParameters> queryParameters = null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String text = null;
			QueryParameters parameters = null;
			while ((text = reader.readLine()) != null) {
				if (text.trim().length() > 0
						&& !(text.trim().substring(0, 1).equals("#"))) {
					if (queryParameters == null) {
						queryParameters = new ArrayList<QueryParameters>(2);
					}
					parameters = getQueryParametersFromString(text);
					if (parameters != null) {
						queryParameters.add(parameters);
					}
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return queryParameters;
	}

	private QueryParameters getQueryParametersFromString(String text)
			throws Exception {
		String[] params = text.split("\t");
		if (params.length != 4) {
			System.out.println("Malformed line: " + text);
			throw new Exception();
		}
		QueryParameters parameters = new QueryParameters();
		parameters.setQueryId(params[0]);
		parameters.setQuery(params[1]);
		parameters.setDuration(Double.parseDouble(params[2]));
		parameters.setQueryParamsPath(params[3]);
		return parameters;
	}

	// Deletes all files and subdirectories under dir.
	// Returns true if all deletions were successful.
	// If a deletion fails, the method stops attempting to delete and returns
	// false.
	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	public static boolean deleteContentsOfDir(File dir) {

		String[] children = dir.list();
		for (int i = 0; i < children.length; i++) {
			boolean success = deleteDir(new File(dir, children[i]));
			if (!success) {
				return false;
			}
		}
		return true;
	}

	private ConnectionDetails getConnectionDetailsFromSystemFile()
			throws URISyntaxException, ParserConfigurationException,
			SAXException, IOException {
		File file = new File(this.getClass().getResource("DBConfig.xml")
				.toURI());
		ConnectionDetails connectionDetails = new ConnectionDetails();
		Document document = parseXML(file);
		NodeList nList = document.getElementsByTagName(DBProperties.DB_CONFIG);
		for (int i = 0; i < nList.getLength(); i++) {
			Node nNode = nList.item(i);
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {

				Element eElement = (Element) nNode;

				connectionDetails.setDbType(getTagValue(DBProperties.DB_DRIVER,
						eElement));
				connectionDetails.setHost(getTagValue(DBProperties.DB_HOST,
						eElement));
				connectionDetails.setPort(getTagValue(DBProperties.DB_PORT,
						eElement));
				connectionDetails.setDbschema(getTagValue(
						DBProperties.DB_SCHEMA, eElement));
				connectionDetails.setDbUserName(getTagValue(
						DBProperties.USER_NAME, eElement));
				connectionDetails.setDbPassword(getTagValue(
						DBProperties.PASSWORD, eElement));

			}
		}
		return connectionDetails;
	}

	public Document parseXML(File file) throws ParserConfigurationException,
			SAXException, IOException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		Document doc = null;

		dBuilder = dbFactory.newDocumentBuilder();

		doc = dBuilder.parse(file);

		doc.getDocumentElement().normalize();
		return doc;
	}

	private String getTagValue(String sTag, Element eElement) {
		NodeList nlList = eElement.getElementsByTagName(sTag).item(0)
				.getChildNodes();
		Node nValue = (Node) nlList.item(0);

		return nValue.getNodeValue();
	}

	private String buildUrl(String dbType, String host, String port,
			String dbschema, String username, String password) {
		StringBuilder url = new StringBuilder("");// "jdbc:mysql://localhost:3306/syllabus";
		if (DBProperties.MYSQL_TYPE.equals(dbType)) {
			url.append(DBProperties.MYSQL_JDBC_STRING);
			url.append(COLON_SEPARATOR);
			url.append(DOUBLE_SLASH_SEPARATOR);
		}
		url.append(host).append(COLON_SEPARATOR).append(port)
				.append(SINGLE_SLASH_SEPARATOR).append(dbschema)
				.append(QUESTION_MARK).append("user=").append(username)
				.append("&password=").append(password);
		return url.toString();
	}

	private class ConnectionDetails {
		private String dbType;
		private String dbUserName;
		private String dbPassword;
		private String dbschema;
		private String host;
		private String port;

		/**
		 * @return the dbType
		 */
		public String getDbType() {
			return dbType;
		}

		/**
		 * @param dbType
		 *            the dbType to set
		 */
		public void setDbType(String dbType) {
			this.dbType = dbType;
		}

		/**
		 * @return the dbUserName
		 */
		public String getDbUserName() {
			return dbUserName;
		}

		/**
		 * @param dbUserName
		 *            the dbUserName to set
		 */
		public void setDbUserName(String dbUserName) {
			this.dbUserName = dbUserName;
		}

		/**
		 * @return the dbPassword
		 */
		public String getDbPassword() {
			return dbPassword;
		}

		/**
		 * @param dbPassword
		 *            the dbPassword to set
		 */
		public void setDbPassword(String dbPassword) {
			this.dbPassword = dbPassword;
		}

		/**
		 * @return the dbschema
		 */
		public String getDbschema() {
			return dbschema;
		}

		/**
		 * @param dbschema
		 *            the dbschema to set
		 */
		public void setDbschema(String dbschema) {
			this.dbschema = dbschema;
		}

		/**
		 * @return the host
		 */
		public String getHost() {
			return host;
		}

		/**
		 * @param host
		 *            the host to set
		 */
		public void setHost(String host) {
			this.host = host;
		}

		/**
		 * @return the port
		 */
		public String getPort() {
			return port;
		}

		/**
		 * @param port
		 *            the port to set
		 */
		public void setPort(String port) {
			this.port = port;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "ConnectionDetails [dbType=" + dbType + ", dbUserName="
					+ dbUserName + ", dbPassword=" + dbPassword + ", dbschema="
					+ dbschema + ", host=" + host + ", port=" + port + "]";
		}

	}
}
