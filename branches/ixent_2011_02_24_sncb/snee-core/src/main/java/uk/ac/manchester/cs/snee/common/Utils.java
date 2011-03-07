/****************************************************************************\ 
 *                                                                            *
 *  SNEE (Sensor NEtwork Engine)                                              *
 *  http://code.google.com/p/snee                                             *
 *  Release 1.0, 24 May 2009, under New BSD License.                          *
 *                                                                            *
 *  Copyright (c) 2009, University of Manchester                              *
 *  All rights reserved.                                                      *
 *                                                                            *
 *  Redistribution and use in source and binary forms, with or without        *
 *  modification, are permitted provided that the following conditions are    *
 *  met: Redistributions of source code must retain the above copyright       *
 *  notice, this list of conditions and the following disclaimer.             *
 *  Redistributions in binary form must reproduce the above copyright notice, *
 *  this list of conditions and the following disclaimer in the documentation *
 *  and/or other materials provided with the distribution.                    *
 *  Neither the name of the University of Manchester nor the names of its     *
 *  contributors may be used to endorse or promote products derived from this *
 *  software without specific prior written permission.                       *
 *                                                                            *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
 *  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
 *  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
 *  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
 *  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
 *                                                                            *
\****************************************************************************/

package uk.ac.manchester.cs.snee.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;



public class Utils {

	static Logger logger = Logger.getLogger(Utils.class.getName());

	/**
	 * Given a relative or absolute filename, ensures file exists.
	 * @param Property name used to specify a file location
	 * @return Full file path is returned
	 * @throws UtilsException 
	 */
	public static String validateFileLocation(String filename) 
	throws UtilsException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER validateFileLocation() with " + filename);
		//Test absolute file location
		File file = new File(filename);
		if (!file.exists()) {
			logger.trace("Absolute file location failed, testing relative path");
			//Absolute file location failed, check relative path
			URL fileUrl = Utils.class.getClassLoader().getResource(filename);
			try {
				file = new File(fileUrl.toURI());
			} catch (Exception e) {
				String message = "Problem reading " +
						filename + " location. Ensure proper path. " +
						file;
				logger.warn(message, e);
				throw new UtilsException(message, e);
			}
		}
		if (!file.exists()) {
			String message = "File location " +
					"specified for " + filename + " does not exist. " +
					"Please provide a valid location.";
			logger.warn(message);
			throw new UtilsException(message);
		}
		String filePath = file.getAbsolutePath();
		if (logger.isTraceEnabled())
			logger.trace("RETURN validateFileLocation() with " + filePath);
		return filePath;
	}

	/**
	 * Checks if a directory exists. If createIfNonExistent is true and 
	 * the directory does not exist, then the directory is created.
	 * @param name directory name to check
	 * @param createIfNonExistent set to true if directory should be created
	 * @throws IOException
	 */
	//TODO: make this throw a UtilsException
	public static void checkDirectory(String name, boolean createIfNonExistent)
	throws IOException {
		File f = new File(name);
		if ((f.exists()) && (!f.isDirectory())) {
			throw new IOException("Directory " + name
					+ "already exists but is not a directory");
		}

		if ((!f.exists() && (!createIfNonExistent))) {
			throw new IOException("Directory " + name + " does not exist");
		}

		if (!f.exists() && (createIfNonExistent)) {
			boolean success = f.mkdirs();
			if (!success) {
				throw new IOException("Directory " + name
						+ " does not exist and cannot be created");
			}
		}
	}

	/**
	 * Delete all the folders and subdirectories of the given directory
	 * @param path
	 * @return
	 */
	public static boolean deleteDirectoryContents(File path) {
		if (path.exists()) {
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectoryContents(files[i]);
					files[i].delete();
				} else {
					files[i].delete();
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Delete the contents of a directory
	 * @param pathName directory name 
	 * @return
	 */
	public static boolean deleteDirectoryContents(String pathName) {
		File path = new File(pathName);
		return deleteDirectoryContents(path);
	}
//
//	public int divideAndRoundUp(int dividend, int divisor) {
//		if ((dividend % divisor) == 0)
//			return dividend / divisor;
//		return dividend / divisor + 1;
//	}
//
	public static int divideAndRoundUp(long dividend, int divisor) {
		if ((dividend % (long) divisor) == 0)
			return (int) (dividend / (long) divisor);
		return (int) (dividend / (long) divisor + 1);
	}

	/**
	 * Pad a string to a specified length
	 * @param s
	 * @param n
	 * @return
	 */
	public static String pad(String s, int n) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < n; i++) {
			result.append(s);
		}
		return result.toString();
	}

	public static String indent(int i) {
		return pad("\t", i);
	}

	/**
	 * Validate the contents of a file against an XML Schema.
	 * @param filename file with XML content to be validated
	 * @param schemaFile XML Schema file
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	//TODO: make this throw a UtilsException
	public static void validateXMLFile(String filename, String schemaFile)
	throws ParserConfigurationException, SAXException, IOException {
		//First validate the XML file according to XML schema file
		// Parse an XML document into a DOM tree.
		DocumentBuilder parser = DocumentBuilderFactory.newInstance()
		.newDocumentBuilder();
		Document document = parser.parse(new File(filename));
		// Create a SchemaFactory capable of understanding WXS schemas.
		SchemaFactory schemaFactory = SchemaFactory
		.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = schemaFactory.newSchema(new File(schemaFile));
		// Create a Validator object, which can be used to validate
		// an instance document.
		Validator validator = schema.newValidator();
		// Validate the DOM tree.
		validator.validate(new DOMSource(document));
	}

	//TODO: make this throw a UtilsException
	public static String doXPathStrQuery(String xmlFile, String query)
	throws XPathExpressionException, FileNotFoundException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		File xmlDocument = new File(xmlFile);
		InputSource inputSource = new InputSource(new FileInputStream(
				xmlDocument));
		xPath.setNamespaceContext(new SNEENamespaceContext());
		String result = xPath.evaluate(query, inputSource);
		if (result.equals(""))
			return null;
		else
			return result;
	}

	//TODO: make this throw a UtilsException
	public static String doXPathStrQuery(Node node, String query)
	throws XPathExpressionException, FileNotFoundException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		xPath.setNamespaceContext(new SNEENamespaceContext());
		String result = (String) xPath.evaluate(query, node,
				XPathConstants.STRING);
		return result;
	}

	//TODO: make this throw a UtilsException
	public static  int doXPathIntQuery(String xmlFile, String query)
	throws XPathExpressionException, FileNotFoundException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		File xmlDocument = new File(xmlFile);
		InputSource inputSource = new InputSource(new FileInputStream(
				xmlDocument));
		xPath.setNamespaceContext(new SNEENamespaceContext());
		String result = xPath.evaluate(query, inputSource);
		if (result.equals(""))
			return -1;
		else
			return Integer.parseInt(result);
	}

	//TODO: make this throw a UtilsException
	public int doXPathIntQuery(Node node, String query)
	throws XPathExpressionException, FileNotFoundException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		xPath.setNamespaceContext(new SNEENamespaceContext());
		int result = (Integer) xPath.evaluate(query, node,
				XPathConstants.NUMBER);
		return result;
	}

	//TODO: make this throw a UtilsException
	public static NodeList doXPathQuery(String xmlFile, String query)
	throws XPathExpressionException, FileNotFoundException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath xPath = factory.newXPath();
		File xmlDocument = new File(xmlFile);
		InputSource inputSource = new InputSource(new FileInputStream(
				xmlDocument));
		xPath.setNamespaceContext(new SNEENamespaceContext());
		NodeList result = (NodeList) xPath.evaluate(query, inputSource,
				XPathConstants.NODESET);
		return result;
	}

	/**
	 * Capitalise the first letter of a string
	 * @param str
	 * @return
	 */
	public static String capFirstLetter(String str) {
		return str.substring(0, 1).toUpperCase() + str.substring(1, str.length());
	}
//
//	public static String fixDirName(final String dirName) {
//		System.out.println("Fix: " + dirName);
//		String fixedDirName = dirName.replaceAll("\\\\", "/");
//		if (!fixedDirName.endsWith("/")) {
//			return fixedDirName + "/";
//		} else {
//			return fixedDirName;
//		}
//	}
//
//	/**
//	 * Converts a value in seconds into days
//	 * @return
//	 */
//	public double convertSecondsToDays(double secs) {
//		return (((secs/60.0)/60.0)/24.0);
//	}
//
//	public static String getFullPath(String relativePath) {
//		System.out.println(relativePath);
//		URL url = Utils.class.getClassLoader().getResource(relativePath);
//		System.out.println("URL: " + url);
//		if (url == null) {
//			System.out.println("RETURN getFullPath() with null");
//			return null;
//		} else {
//			String fileName = url.getFile();
//			System.out.println("RETURN getFullPath() with " + fileName);
//			return fileName;
//		}
//	}
	
	public static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
		}
	
	/**
	 * Runs an external program. Waits until it has finished executing.
	 * @param progName
	 * @param params
	 * @param env
	 * @throws IOException
	 */
	public static String runExternalProgram(String progName, String[] params, 
	Map<String,String> extraEnvVars, String workingDir) throws IOException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER runExternalProgram()");	
		
		String cmdarray[] = concat(new String[]{progName}, params);
		logger.info("Command array="+Arrays.toString(cmdarray));

		ProcessBuilder pb = new ProcessBuilder(cmdarray);
		pb.redirectErrorStream(true);
		pb.directory(new File(workingDir));
		Map<String,String> env = pb.environment();
		env.putAll(extraEnvVars);
		
		Process proc = pb.start();
	    final InputStream is = proc.getInputStream();
	    final InputStreamReader isr = new InputStreamReader(is);
	    final BufferedReader br = new BufferedReader(isr);
	    String line;

	    StringBuffer output = new StringBuffer();
	    while ((line = br.readLine()) != null) {
	    	logger.trace(line);
	    	System.out.println(line);
	    	output.append(line + "\n");
	    }
	    

	    try {
			proc.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if (proc.exitValue()!=0) {
	    	System.err.println("an error has occurred");
	    	System.exit(-1);
	    }
	    
	    if (logger.isDebugEnabled())
			logger.debug("RETURN runExternalProgram()");
	    return output.toString();
	}


	/**
	 * Given a resource in the class loader search path, returns an absolute path to this resource.
	 * @param relativeResourcePath
	 * @return
	 */
	public static String getResourcePath(String relativeResourcePath) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getResourcePath()");
		URL url = Utils.class.getClassLoader().getResource(relativeResourcePath);
		File file = new File(url.getFile());
		if (logger.isDebugEnabled())
			logger.debug("RETURN getResourcePath()");		
		return file.toString();
	}

	/**
	 * Reads a file into a string.
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	//taken from: http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file
	public static String readFileToString(String path) throws IOException {
		  FileInputStream stream = new FileInputStream(new File(path));
		  try {
		    FileChannel fc = stream.getChannel();
		    MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
		    /* Instead of using default, pass in a decoder. */
		    return Charset.defaultCharset().decode(bb).toString();
		  }
		  finally {
		    stream.close();
		  }
		}
	
}
