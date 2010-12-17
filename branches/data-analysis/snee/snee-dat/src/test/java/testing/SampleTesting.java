package testing;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.IException;
import gr.uoa.di.ssg4e.query.IMetadata;
import gr.uoa.di.ssg4e.query.QueryRefactorer;
import gr.uoa.di.ssg4e.query.excep.ParserException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

public class SampleTesting {

	public static final String schemaName = "java/resources/logical-schema.xml";

	public static final String baseClassPath = 
		"/home/lebiathan/.m2/repository/uk/ac/manchester/cs/snee/snee-core/1.4.2/snee-core-1.4.2.jar";

	public static final String metadataClassName = 
		"uk.ac.manchester.cs.snee.compiler.metadata.Metadata";

	public static final String sneePropertyNamesClassName = 
		"uk.ac.manchester.cs.snee.common.SNEEPropertyNames";

	public static final String sneePropertiesClassName = 
		"uk.ac.manchester.cs.snee.common.SNEEProperties";

	public static final String baseDirectory = "../compiler/src/test/resources/etc/";

	public static void main( String[] args ){

		try {

			/* These actions must be performed in order to correctly initialize any items that
			 * are needed by the Metadata class */
			URL[] us = {new URL("file:" + baseClassPath)};
			ClassLoader ucl = new URLClassLoader(us);
			Class sneePropNamesClass = ucl.loadClass(sneePropertyNamesClassName);

			Properties props = new Properties();
			props.setProperty(
					sneePropNamesClass.getField("INPUTS_TYPES_FILE").get(null).toString(), baseDirectory + "Types.xml");
			props.setProperty(
					sneePropNamesClass.getField("INPUTS_UNITS_FILE").get(null).toString(), baseDirectory + "units.xml");
			props.setProperty(
					sneePropNamesClass.getField("INPUTS_LOGICAL_SCHEMA_FILE").get(null).toString(), baseDirectory + "logical-schema.xml");
			props.setProperty(
					sneePropNamesClass.getField("INPUTS_PHYSICAL_SCHEMA_FILE").get(null).toString(), baseDirectory + "physical-schema.xml");
			props.setProperty(
					sneePropNamesClass.getField("INPUTS_COST_PARAMETERS_FILE").get(null).toString(), baseDirectory + "cost-parameters.xml");
			props.setProperty(
					sneePropNamesClass.getField("GENERAL_OUTPUT_ROOT_DIR").get(null).toString(), "output");
			props.setProperty(
					sneePropNamesClass.getField("SNCB_PERFORM_METADATA_COLLECTION").get(null).toString(), "false");
			props.setProperty(
					sneePropNamesClass.getField("SNCB_GENERATE_COMBINED_IMAGE").get(null).toString(), "false");

			Class sneePropsClass = ucl.loadClass(sneePropertiesClassName);
			Method initMethod = sneePropsClass.getMethod(
					"initialise", new Class[] { Properties.class });
			initMethod.invoke(null, props);

			ucl = new URLClassLoader(us, IMetadata.class.getClassLoader());
			IMetadata metadata = (IMetadata)ucl.loadClass(metadataClassName).newInstance();

			QueryRefactorer refactor = new QueryRefactorer(metadata);

			String query = null; /* initial query */
			String refQuery = null; /* refactored query */

			/* Test the following sample queries! */
			query = Queries.q1;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q2;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q3;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q4;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q41;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q13;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q14;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");

			query = Queries.q15;
			System.out.println("\n");
			System.out.println(query);
			refQuery = refactor.refactorQuery(query);
			System.out.println(refQuery);
			System.out.println("==============================================");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
