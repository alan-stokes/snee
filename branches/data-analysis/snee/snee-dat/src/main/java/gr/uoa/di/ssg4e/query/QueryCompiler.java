/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.allocator.SourceAllocatorException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.ExpressionException;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import antlr.RecognitionException;
import antlr.TokenStreamException;

/**
 * 
 */
public class QueryCompiler extends
		uk.ac.manchester.cs.snee.compiler.QueryCompiler {

	private QueryRefactorer refactor = null;

	public QueryCompiler(Metadata schema) 
	throws TypeMappingException {
		super(schema);
		refactor = new QueryRefactorer(schema);
	}

	/**
	 * Compile a query into the logical algebraic form for processing
	 * by the out of network query evaluation engine.
	 * 
	 * @param queryID The identifier for the query
	 * @param query The query text
	 * @return logical algebraic form of the query
	 * @throws SNEEException 
	 * @throws OptimizationException 
	 * @throws  
	 * @throws ParserValidationException 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 * @throws SourceDoesNotExistException 
	 * @throws ExtentDoesNotExistException 
	 * @throws TokenStreamException 
	 * @throws RecognitionException 
	 * @throws SNEEConfigurationException 
	 * @throws SourceAllocatorException 
	 * @throws SourceAllocatorException 
	 * @throws WhenSchedulerException 
	 * @throws ExpressionException 
	 * @throws gr.uoa.di.ssg4e.query.excep.ParserException 
	 * @throws gr.uoa.di.ssg4e.query.excep.ParserException 
	 * @throws DATException 
	 */
	public QueryExecutionPlan compileQuery(int queryID, String query, 
			QoSExpectations qos) 
	throws SNEEException, SourceDoesNotExistException, 
	TypeMappingException, SchemaMetadataException, OptimizationException, 
	ParserException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException, 
	SNEEConfigurationException, SourceAllocatorException, WhenSchedulerException,
	ExpressionException, gr.uoa.di.ssg4e.query.excep.ParserException, DATException 
	 {
		query = refactor.refactorQuery(query);
		return super.compileQuery(queryID, query, qos);
	}

}
