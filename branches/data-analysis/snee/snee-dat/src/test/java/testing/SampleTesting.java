package testing;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.QueryRefactorer;
import gr.uoa.di.ssg4e.query.excep.ParserException;

import java.net.MalformedURLException;

import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.MetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;

public class SampleTesting {

	public static final String schemaName = "java/resources/logical-schema.xml";

	public static void main( String[] args ){

		try {

			Metadata metadata = new Metadata(schemaName);
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
		} catch (TypeMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SchemaMetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedAttributeTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DATException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
