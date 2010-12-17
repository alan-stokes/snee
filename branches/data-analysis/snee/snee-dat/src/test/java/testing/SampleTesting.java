package testing;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.IMetadata;
import gr.uoa.di.ssg4e.query.QueryRefactorer;
import gr.uoa.di.ssg4e.query.excep.ParserException;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class SampleTesting {

	public static final String schemaName = "java/resources/logical-schema.xml";

	public static final String baseClassPath = 
		"/home/lebiathan/.m2/repository/uk/ac/manchester/cs/snee/snee-core/1.4.2/snee-core-1.4.2.jar";

	public static final String className = 
		"uk.ac.manchester.cs.snee.compiler.metadata.Metadata";

	public static void main( String[] args ){

		try {

			/* These actions need to be performed in order to correctly initialize any items that
			 * are needed by the Metadata class */
//			props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
//			props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
//			props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
//			SNEEProperties.initialise(props);

			URL[] us = {new URL("file:" + baseClassPath)};
			ClassLoader ucl = new URLClassLoader(us, IMetadata.class.getClassLoader());
			IMetadata metadata = (IMetadata)ucl.loadClass(className).newInstance();

//			IMetadata metadata = (IMetadata) metadataClass
//			Method myMethod = metadataClass.getMethod("Metadata",
//					new Class[] { String.class });
//			String returnValue = (String) myMethod.invoke(metadata,
//					new Object[] {  });

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
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DATException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
