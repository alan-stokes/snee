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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClientUsingTupleGeneratorForJoin;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

/**
 * @author Praveen
 * 
 * This class copies and replaces some files, so please
 * be careful while using this file
 * 
 */
public class TestClient {

	private SNEEClientUsingTupleGeneratorForJoin clientUsingTupleGeneratorForJoin = null;
	//private static ConstantRatePushStreamGenerator _myDataSource;
	private static final String ORIGINAL_FILE = "Original";
	private static final String OUTPUT_PATH = "exp_output";
	protected static Logger logger = Logger.getLogger(TestClient.class
			.getName());

	public static void main(String[] args) {

		try {
			// File file = new File(args[0]);

			/*
			 * if (!file.canRead()) {
			 * System.out.println("Unable to read the file"); }
			 */
			PropertyConfigurator.configure(TestClient.class.getClassLoader()
					.getResource("etc/log4j.properties"));

			TestClient testClient = new TestClient();
			/*
			 * Collection<QueryParameters> queryParameters = testClient
			 * .getQueryParametersfromFile(file); if (queryParameters != null &&
			 * queryParameters.size() > 0) { executeQueries(queryParameters); }
			 */
			Collection<QueryParameters> queryParameters = testClient
					.readInput();
			if (queryParameters != null && queryParameters.size() > 0) {
				testClient.executeEachQuery(queryParameters);
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

	private void executeEachQuery(Collection<QueryParameters> queryParameters)
			throws URISyntaxException, IOException {
		String prevQueryId = ORIGINAL_FILE;
		String outputPath = OUTPUT_PATH;
		File outputLocation = new File(outputPath);
		if (!outputLocation.exists()) {
			outputLocation.mkdir();
		}
		deleteContentsOfDir(outputLocation);

		/*
		 * if (!outputLocation.exists()) { outputLocation.mkdir(); }
		 */
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
			executeQuery(parameters);
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
	 */
	private void executeQuery(QueryParameters parameters) {
		PropertyConfigurator.configure(TestClient.class.getClassLoader()
				.getResource("etc/log4j.properties"));		
		try {
			logger.info("Starting Query " + parameters.getQuery()
					+ " with duration " + parameters.getDuration()
					+ " and experiment path " + parameters.getQueryParamsPath());
			clientUsingTupleGeneratorForJoin = new SNEEClientUsingTupleGeneratorForJoin(
					parameters.getQuery(), parameters.getDuration(),
					"etc/query-parameters.xml");

			/*Process process= Runtime.getRuntime().exec("TestTupleGenerator.cmd", new String[]{Double.toString(parameters.getDuration())});
			if (process.exitValue() != 0) {
				InputStream is = process.getInputStream();
			    InputStreamReader isr = new InputStreamReader(is);
			    BufferedReader br = new BufferedReader(isr);
			    String line;

			    StringBuffer output = new StringBuffer();
			    while ((line = br.readLine()) != null) {
			    	logger.trace(line);
			    	System.out.println(line);
			    	output.append(line + "\n");
			    }
			    
			    is = process.getErrorStream();
			    isr = new InputStreamReader(is);
			    br = new BufferedReader(isr);			    

			    output = new StringBuffer();
			    while ((line = br.readLine()) != null) {
			    	logger.trace(line);
			    	System.out.println(line);
			    	output.append(line + "\n");
			    }
			}*/
			/* Initialise and run data source */
			//_myDataSource = new ConstantRatePushStreamGenerator();
			//_myDataSource.startTransmission();
			clientUsingTupleGeneratorForJoin.run();
			//_myDataSource.stopTransmission();

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
		} /*catch (TypeMappingException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SchemaMetadataException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (UnsupportedAttributeTypeException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SourceMetadataException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (TopologyReaderException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (CostParametersException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		} catch (SNCBException e) {
			logger.fatal("Execution failed", e);
			e.printStackTrace();
		}*/ catch (SNEECompilerException e) {
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
			//_myDataSource = null;
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
					// System.out.println("Copying file: " + file);
					InputStream in = new FileInputStream(file);
					/*
					 * File newFile = new File(targetLocation + "/" +
					 * file.getName()); if (newFile.exists()) {
					 * System.out.println("file Exists: " + newFile);
					 * newFile.delete(); }
					 */
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

	public static void main1(String[] args) throws SNEEException, IOException,
			SNEEConfigurationException, MetadataException,
			SNEEDataSourceException, TypeMappingException,
			SchemaMetadataException, UnsupportedAttributeTypeException,
			SourceMetadataException, TopologyReaderException,
			CostParametersException, SNCBException, SNEECompilerException,
			EvaluatorException, SQLException {
		if (args.length < 1) {
			System.out.println("Please specify the query file as argument");
			return;
		}
		File file = new File(args[0]);

		if (!file.canRead()) {
			System.out.println("Unable to read the file");
		}
		PropertyConfigurator.configure(TestClient.class.getClassLoader()
				.getResource("etc/log4j.properties"));

		TestClient testClient = new TestClient();
		Collection<QueryParameters> queryParameters = testClient
				.getQueryParametersfromFile(file);
		if (queryParameters != null && queryParameters.size() > 0) {
			executeQueries(queryParameters);
		}

	}

	private static void executeQueries(
			Collection<QueryParameters> queryParameters) throws SNEEException,
			IOException, SNEEConfigurationException, MetadataException,
			SNEEDataSourceException, TypeMappingException,
			SchemaMetadataException, UnsupportedAttributeTypeException,
			SourceMetadataException, TopologyReaderException,
			CostParametersException, SNCBException, SNEECompilerException,
			EvaluatorException, SQLException {
		SNEEClientUsingTupleGeneratorForJoin clientUsingTupleGeneratorForJoin = null;		
		for (QueryParameters parameters : queryParameters) {
			PropertyConfigurator.configure(TestClient.class.getClassLoader()
					.getResource("etc/log4j.properties"));
			clientUsingTupleGeneratorForJoin = new SNEEClientUsingTupleGeneratorForJoin(
					parameters.getQuery(), parameters.getDuration(),
					parameters.getQueryParamsPath());
			/* Initialise and run data source */
			//_myDataSource = new ConstantRatePushStreamGenerator();
			//_myDataSource.startTransmission();
			clientUsingTupleGeneratorForJoin.run();
			//_myDataSource.stopTransmission();
			//_myDataSource = null;
		}

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
}
