package uk.ac.manchester.cs.snee.common;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SNEEProperties {

	/**
	 * Logger for this class.
	 */
	private static Logger logger = 
		Logger.getLogger(SNEEProperties.class.getName());
	
	private static Properties _props;	

	public static void initialise(Properties props) throws SNEEConfigurationException {
		if (logger.isInfoEnabled()) {
			logger.info("ENTER initialise() #props=" + props.size());
		}
		_props = new Properties(props);
//		_props.list(System.out);
		validateProperties();
		if (logger.isDebugEnabled()) {
			logProperties();
		}
		if (logger.isInfoEnabled()) {
			logger.info("RETURN initialise()");
		}
	}

	private static void logProperties() {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER logProperties()");
		}
		StringBuffer msg = new StringBuffer("Properties (#props=" + _props.stringPropertyNames().size() + "):\n");
		Enumeration<?> propNames = _props.propertyNames();
		while (propNames.hasMoreElements()) {
			String propName = (String) propNames.nextElement();
			msg.append(propName).append("=").append(_props.getProperty(propName)).append("\n");
		}
		if (logger.isDebugEnabled()) {
			logger.debug(msg.toString());
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN logProperties()");
		}
	}

	private static void validateProperties() throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER validateProperties()");
		validateGraphVizSettings();
		validateFileLocation(SNEEPropertyNames.INPUTS_TYPES_FILE);
		validateFileLocation(SNEEPropertyNames.INPUTS_UNITS_FILE);
		validateDir(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, true);
		if (logger.isDebugEnabled())
			logger.debug("RETURN validateProperties()");
	}

	/**
	 * Ensure graphviz is available if we are trying to generate graphs
	 * @throws SNEEConfigurationException
	 */
	private static void validateGraphVizSettings()
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER validateGraphVizSettings()");
		String generateGraphs = 
			_props.getProperty(SNEEPropertyNames.GENERAL_GENERATE_GRAPHS);
		logger.info(generateGraphs);
		if (generateGraphs != null &&
				generateGraphs.equals("true") &&
				_props.getProperty(SNEEPropertyNames.GRAPHVIZ_EXE) == null) {
			String message = "Need to provide the " +
				"graphviz.exe location for the graphviz executable " +
				"if compiler.gernate_graphs is set to true.";
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN validateGraphVizSettings()");
	}
	
	/**
	 * Ensure location has been specified and file exists
	 * @param Property name used to specify a file location
	 * @return Full file path is returned
	 * @throws SNEEConfigurationException types file location has not been specified or the file does not exist
	 */
	private static String validateFileLocation(String propName) 
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER validateFileLocation() with " + propName);		
		String fileName = _props.getProperty(propName);
		if (fileName == null) {
			String message = "A file location must " +
					"be specified for " + propName + ".";
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		logger.debug(propName + " exists as a system property");
		//Test absolute file location
		File file = new File(fileName);
		if (!file.exists()) {
			logger.trace("Absolute file location failed, testing relative path");
			//Absolute file location failed, check relative path
			URL fileUrl = SNEEProperties.class.getClassLoader().getResource(fileName);
			try {
				file = new File(fileUrl.toURI());
			} catch (URISyntaxException e) {
				String message = "Problem reading " +
						propName + " location. Ensure proper path. " +
						file;
				logger.warn(message);
				throw new SNEEConfigurationException(message);
			}
		}
		if (!file.exists()) {
			String message = "File location " +
					"specified for " + propName + " does not exist. " +
					"Please provide a valid location.";
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		String filePath = file.getAbsolutePath();
		if (logger.isDebugEnabled())
			logger.debug("RETURN validateFileLocation() with " + filePath);
		return filePath;
	}

	/**
	 * Checks that a directory name has been specified. If directory 
	 * does not exist and create is set to true, the directory will
	 * be created relative to the current working directory.
	 * @param propName name of the property that contains a directory name
	 * @param create controls if a directory should be created
	 * @throws SNEEConfigurationException directory name has not been specified
	 */
	private static void validateDir(String propName, Boolean create) 
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER validateDir() with " + propName + 
					" create=" + create);
		String dirName = _props.getProperty(propName);
		//Fail if property has not been set
		if (dirName == null) {
			String message = "Directory " + dirName + " not specified.";
			throw new SNEEConfigurationException(message);
		}
		if (logger.isTraceEnabled())
			logger.trace("Directory path: " + dirName);
		File dir = new File(dirName);
		if (!dir.exists()) {
			logger.trace("Absolute directory location failed, testing relative path");
			//Absolute directory location failed, check relative path
			URL dirUrl = SNEEProperties.class.getClassLoader().getResource(dirName);
			if (dirUrl != null) {
				try {
					dir = new File(dirUrl.toURI());
				} catch (URISyntaxException e) {
					String message = "Problem reading " +
					propName + " location. Ensure proper path. " +
					dir;
					logger.warn(message);
					throw new SNEEConfigurationException(message);
				}
			} else {
				if (create) {
					logger.trace("Creating directory " + dir.getAbsolutePath());
					if (!dir.mkdir()) {
						String message = "Problem creating " + dirName;
						logger.warn(message);
						throw new SNEEConfigurationException(message);
					}
				}
			}
		}
		if (!dir.exists()) {
			String message = "Problem reading " +
				propName + " location. Ensure proper path. " +
				dir;
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN validateDir() " + dir);
	}

	/**
	 * Test if a property has been set
	 * @param propName property name to test
	 * @return true if property has be set, otherwise false
	 */
	public static boolean isSet(String propName) {
		if (logger.isInfoEnabled())
			logger.info("ENTER isSet() with " + propName);
		boolean result = false;
		if (_props.getProperty(propName) != null)
			result = true;
		if (logger.isInfoEnabled())
			logger.info("RETURN isSet() with " + result);
		return result;
	}
	
	/**
	 * Returns the property value for a property name
	 * @param propName name of the property to retrieve the value for
	 * @return value of the specified property
	 * @throws SNEEConfigurationException property does not exist
	 */
	public static String getSetting(String propName) 
	throws SNEEConfigurationException {
		if (logger.isInfoEnabled())
			logger.info("ENTER getSetting() with " + propName);
		String property = _props.getProperty(propName);
		if (property == null) {
			String message = "Unknown property " + propName;
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		if (logger.isInfoEnabled())
			logger.info("RETURN getSetting() " + propName + " found.");
		return property;
	}
	
	public static String getFilename(String propertyName) 
	throws SNEEConfigurationException {
		if (logger.isInfoEnabled())
			logger.info("ENTER getFile() with " + propertyName);
		String fileName = validateFileLocation(propertyName);
		if (logger.isInfoEnabled())
			logger.info("RETURN getFile() with " + fileName);
		return fileName;
	}
}
