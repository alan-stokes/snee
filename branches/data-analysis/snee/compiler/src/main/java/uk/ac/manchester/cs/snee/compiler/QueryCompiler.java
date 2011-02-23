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

package uk.ac.manchester.cs.snee.compiler;

import gr.uoa.di.ssg4e.dat.excep.DATException;
import gr.uoa.di.ssg4e.query.IException;
import gr.uoa.di.ssg4e.query.IMetadata;
import gr.uoa.di.ssg4e.query.QueryRefactorer;

import java.io.IOException;
import java.io.StringReader;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.allocator.SourceAllocator;
import uk.ac.manchester.cs.snee.compiler.allocator.SourceAllocatorException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;
import uk.ac.manchester.cs.snee.compiler.planner.SourcePlanner;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.ExpressionException;
import uk.ac.manchester.cs.snee.compiler.rewriter.LogicalRewriter;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.translator.Translator;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;

/**
 * Main class for SNEEql Query Optimizer.
 * Compiles a declarative SNEEql query into a query execution plan.
 */
public class QueryCompiler {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(QueryCompiler.class.getName());

	/**
	 * The metadata being used. 
	 */
	private MetadataManager metadata;

	private QueryRefactorer refactor = null;

	public QueryCompiler(MetadataManager schema) 
	throws TypeMappingException {
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER QueryCompiler()");
		metadata = schema;
		refactor = new QueryRefactorer((IMetadata)metadata);
		if (logger.isDebugEnabled())
			logger.debug("RETURN QueryCompiler()");
	}
	
	private CommonAST doParsing(String query, int queryID, String outputDir) 
	throws ParserException, RecognitionException, TokenStreamException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doParsing() with queryID: " + queryID +
					" dir: " + outputDir + " query:\n\t" + query);
		SNEEqlLexer lexer = new SNEEqlLexer(new StringReader(query)); 
		SNEEqlParser parser = new SNEEqlParser(lexer);
		parser.parse();
		CommonAST parseTree = (CommonAST)parser.getAST();
		if (logger.isTraceEnabled())
			logger.trace("RETURN doParsing() with Parse tree: " + 
					parseTree.toStringList());
		return parseTree;
	}
	
	private LAF doTranslation(CommonAST parseTree, int queryID, 
			String queryPlanOutputDir) 
	throws TypeMappingException, SourceDoesNotExistException, 
	SchemaMetadataException, 
	OptimizationException, ParserException, ExtentDoesNotExistException,
	RecognitionException, SNEEConfigurationException, ExpressionException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER doTranslation() queryID: " + 
					queryID + "\n\tquery: " + parseTree);
		Translator translator = new Translator(metadata);
		LAF laf = translator.translate(parseTree, queryID);    

//				qosCollection.get(queryID).getMaxAcquisitionInterval());
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new LAFUtils(laf).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN doTranslation() with  " + laf);
		return laf;

	}

	private LAF doLogicalRewriting(LAF laf, int queryID) 
	throws SNEEConfigurationException, OptimizationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doLogicalRewriting: " + laf);
		LogicalRewriter rewriter = new LogicalRewriter();
		LAF lafPrime = rewriter.doLogicalRewriting(laf);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new LAFUtils(lafPrime).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN doLogicalRewriting() with " +
					lafPrime);
		return lafPrime;		
	}
	
		
	private DLAF doSourceAllocation(LAF lafPrime, int queryID) throws 
	SourceAllocatorException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doSourceAllocation: " + lafPrime);
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(lafPrime);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new DLAFUtils(dlaf).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN doSourceAllocation: " + dlaf);
		return dlaf;
	}
	
	private QueryExecutionPlan doSourcePlanning(DLAF dlaf, QoSExpectations qos, 
	int queryID) 
	throws SNEEException, SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
	OptimizationException, WhenSchedulerException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doSourcePlanning: " + dlaf);
		SourcePlanner planner = new SourcePlanner(metadata);
		QueryExecutionPlan qep = planner.doSourcePlanning(dlaf, qos,
			metadata.getCostParameters(), queryID);
		if (logger.isTraceEnabled())
			logger.trace("RETURN doSourcePlanning");
		return qep;
	}

	/**
	 * Compile a query into the logical algebraic form for processing
	 * by the out of network query evaluation engine.
	 * 
	 * @param queryID The identifier for the query
	 * @param query The query text
	 * @return logical algebraic form of the query
	 * @throws SNEEException 
	 * @throws ParserException 
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
	 * @throws DATException 
	 */
	public QueryExecutionPlan compileQuery(int queryID, String query, 
			QoSExpectations qos) 
	throws SNEEException, SourceDoesNotExistException, 
	TypeMappingException, SchemaMetadataException, OptimizationException, 
	ParserException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException, 
	SNEEConfigurationException, SourceAllocatorException, WhenSchedulerException,
	ExpressionException, 
	gr.uoa.di.ssg4e.query.excep.ParserException, DATException 
	 {
		if (logger.isDebugEnabled())
			logger.debug("ENTER: queryID: " + queryID + "\n\tquery: " + query);
		logger.info("Compiling queryID " + queryID);
		String outputDir = createQueryDirectory(queryID);

		if (logger.isTraceEnabled())
			logger.trace("ENTER: queryID: " + queryID + 
					" dir: " + outputDir + "\n\tquery: " + query);

		if (logger.isInfoEnabled()) 
			logger.info("Starting refactoring for queryID " + queryID);
		try{
			query = refactor.refactorQuery(query);
			if ( !query.endsWith(";") )
				query = query + ";";
			System.out.println("Refactored Query: " + query);
		}catch(IException ie){
			throw new SchemaMetadataException(ie.getMessage());
		}

		if (logger.isInfoEnabled()) 
			logger.info("Starting parsing for queryID " + queryID);
		CommonAST ast = doParsing(query, queryID, outputDir);

		if (logger.isInfoEnabled()) 
			logger.info("Starting Translation for query " + queryID);
		LAF laf = doTranslation(ast, queryID, outputDir);
		
		if (logger.isInfoEnabled()) 
			logger.info("Starting Logical Rewriting for query " + queryID);
		LAF lafPrime = doLogicalRewriting(laf, queryID);
		
		if (logger.isInfoEnabled()) 
			logger.info("Starting Source Allocation for query " + queryID);
		DLAF dlaf = doSourceAllocation(lafPrime, queryID);
		
		if (logger.isInfoEnabled()) 
			logger.info("Starting Source Planner for query " + queryID);
		QueryExecutionPlan qep = doSourcePlanning(dlaf, qos, queryID);

		if (logger.isDebugEnabled())
			logger.debug("RETURN: " + qep.getID());
		return qep;
	}

	private String createQueryDirectory(int queryID) 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createQueryDirectory() queryID: " + queryID);
		String queryPlanOutputDir;
		try {
			Utils.deleteDirectoryContents(SNEEProperties.getSetting(
					SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR));
			String fileSeparator = System.getProperty("file.separator");
			String outputRootDir = 
				SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
				fileSeparator + "query" + queryID + fileSeparator; 
			queryPlanOutputDir = outputRootDir + Constants.QUERY_PLAN_DIR;
			Utils.checkDirectory(queryPlanOutputDir, true);
		} catch (IOException e) {
			String msg = "Unable to create directory for query " + queryID;
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN createQueryDirectory() " + queryPlanOutputDir);
		return queryPlanOutputDir;
	}


}
