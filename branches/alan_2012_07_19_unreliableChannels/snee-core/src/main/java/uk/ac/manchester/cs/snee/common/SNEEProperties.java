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
	private static final Logger logger = Logger.getLogger(SNEEProperties.class.getName());
	
	private static Properties _props;	

	public static void initialise(Properties props) 
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER initialise() #props=" + props.size());
		}
		_props = new Properties(props);
//		_props.list(System.out);
		validateProperties();
		if (logger.isDebugEnabled()) {
			logProperties();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN initialise()");
		}
	}

	private static void logProperties() {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER logProperties()");
		}
		StringBuffer msg = 
			new StringBuffer("Properties (#props=" + 
					_props.stringPropertyNames().size() + "):\n");
		Enumeration<?> propNames = _props.propertyNames();
		while (propNames.hasMoreElements()) {
			String propName = (String) propNames.nextElement();
			msg.append(propName).append("=");
			msg.append(_props.getProperty(propName)).append("\n");
		}
		if (logger.isDebugEnabled()) {
			logger.debug(msg.toString());
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN logProperties()");
		}
	}

	private static void validateProperties() throws SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER validateProperties()");
		validateGraphVizSettings();
		validateDir(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, true);
		if (isSet(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE)) {
			validatePropertyFileLocation(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE);			
		}
		if (isSet(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE)) {
			validatePropertyFileLocation(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE);
		}
		if (isSet(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE)) {
			validatePropertyFileLocation(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE);			
		}
		validatePropertyFileLocation(SNEEPropertyNames.INPUTS_TYPES_FILE);
		validatePropertyFileLocation(SNEEPropertyNames.INPUTS_UNITS_FILE);
		if (logger.isTraceEnabled())
			logger.trace("RETURN validateProperties()");
	}

  /**
	 * Ensure graphviz is available if we are trying to generate graphs
	 * @throws SNEEConfigurationException
	 */
	private static void validateGraphVizSettings()
	throws SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER validateGraphVizSettings()");
		String generateGraphs = 
			_props.getProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES);
		String convertGraphs =
			_props.getProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES);
		if (generateGraphs != null &&
				generateGraphs.equals("true") &&
				convertGraphs != null &&
				convertGraphs.equals("true") &&
				_props.getProperty(SNEEPropertyNames.GRAPHVIZ_EXE) == null) {
			String message = "Need to provide the " +
				"graphviz.exe location for the graphviz executable " +
				"if compiler.gernate_graphs is set to true.";
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN validateGraphVizSettings()");
	}
	
	/**
	 * Ensure location has been specified and file exists
	 * @param Property name used to specify a file location
	 * @return Full file path is returned
	 * @throws SNEEConfigurationException types file location 
	 * has not been specified or the file does not exist
	 */
	private static String validatePropertyFileLocation(String propName) 
	throws SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER validatePropertyFileLocation() with " 
					+ propName);		
		String filename = _props.getProperty(propName);
		if (filename == null) {
			String message = "A file location must " +
					"be specified for " + propName + ".";
			logger.warn(message);
			throw new SNEEConfigurationException(message);
		}
		if (logger.isDebugEnabled()) {
			logger.debug(propName + " exists as a system property");
		}
		try {
			String filePath = Utils.validateFileLocation(filename);
			if (logger.isTraceEnabled())
				logger.trace("RETURN validatePropertyFileLocation() with " + 
						filePath);
			return filePath;			
		} catch (UtilsException e) {
			String msg = "Problem with property " + propName + ": ";
			logger.warn(msg, e);
			throw new SNEEConfigurationException(msg + e.getMessage(), e);
		}
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
		if (logger.isTraceEnabled())
			logger.trace("ENTER validateDir() with " + propName + 
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
					logger.warn(message, e);
					throw new SNEEConfigurationException(message);
				}
			} else {
				if (create) {
					if (logger.isTraceEnabled()) {
						logger.trace("Creating directory " + 
								dir.getAbsolutePath());
					}
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
		if (logger.isTraceEnabled())
			logger.trace("RETURN validateDir() " + dir);
	}

	/**
	 * Test if a property has been set
	 * @param propName property name to test
	 * @return true if property has be set, otherwise false
	 */
	public static boolean isSet(String propName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER isSet() with " + propName);
		boolean result = false;
		if (_props.getProperty(propName) != null)
			result = true;
		if (logger.isDebugEnabled())
			logger.debug("RETURN isSet() with " + result);
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
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSetting() with " + propName);
		String property = _props.getProperty(propName);
		if (property == null)
		{
		  property = getDefaultProperty(propName);
		  if(property == null)
		  {
  			String message = "Unknown property \'" + propName + "\'";
  			logger.warn(message);
  			throw new SNEEConfigurationException(message);
		  }
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getSetting() " + propName + 
					" = " + property);
		return property;
	}
	
	public static void setSetting(String propName, String setting)
	{
	  _props.remove(propName);
	  _props.setProperty(propName, setting);
	}
	
	private static String getDefaultProperty(String propName) 
	throws SNEEConfigurationException
  {
    if(propName.equals("wsn_manager.strategies"))
    {
      setSetting(propName,"FailedNodePartial");
      return getSetting(propName);
    }
    else if(propName.equals("choiceAssessorPreference"))
    {
      setSetting(propName, "Best");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.k_resilence_level"))
    {
      setSetting(propName, "0");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.k_resilence_sense"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.successor"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels"))
    {
      setSetting(propName, "true");
      return getSetting(propName);
    }
    else if(propName.equals("runCostModel"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("runSimFailedNodes"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("runWithFailures"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("avroraRealTime"))
    {
      setSetting(propName, "true");
      return getSetting(propName);
    }
    else if(propName.equals("runAvrora"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.setup_frameworks"))
    {
      setSetting(propName, "true");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.clean.radio"))
    {
      setSetting(propName, "false");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.resilientlevel"))
    {
      setSetting(propName, "2");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.redundantcycles"))
    {
      setSetting(propName, "1");
      return getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.noiseModel"))
    {
      setSetting(propName, "src/main/resources/noiseFiles/meyer-heavy.txt");
      return  getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.transmissionPower"))
    {
      setSetting(propName, "-12");
      return  getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.veriableTransmissionPower"))
    {
      setSetting(propName, "true");
      return  getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.signalFrequency"))
    {//in megahertz
      setSetting(propName, "2400");
      return  getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.pathLossExponent"))
    {
      setSetting(propName, "2");
      return  getSetting(propName);
    }
    else if(propName.equals("wsn_manager.unreliable.channels.simulationLength"))
    {
      setSetting(propName, "200");
      return  getSetting(propName);
    }
    
    
    
    
    
    else return null;
  }

  public static int getIntSetting(String propName) 
	throws SNEEConfigurationException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getIntSetting() with " + propName);
		}
		int property;
		try {
			property = new Integer(getSetting(propName));
		} catch (NumberFormatException e) {
			String message = "Problem converting property " +
					"value to an integer.";
			logger.warn(message, e);
			throw new SNEEConfigurationException(message, e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getIntSetting() " + propName + 
					" = " + property);
		return property;
	}
	
	/**
	 * Returns the property value for a boolean property
	 * @param propName name of the property to retrieve the value for
	 * @return value of the specified property
	 * @throws SNEEConfigurationException property does not exist
	 */
	public static boolean getBoolSetting(String propName)
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getBoolSetting() with " + propName);
		String property = getSetting(propName);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getBoolSetting() " + 
					propName + "="+property);
		return property.toUpperCase().equals("TRUE");
	}
	
	public static String getFilename(String propertyName) 
	throws SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getFile() with " + propertyName);
		String fileName = validatePropertyFileLocation(propertyName);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getFile() with " + fileName);
		return fileName;
	}
}
