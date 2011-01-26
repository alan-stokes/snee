package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public interface SourceReceiver {
	
	/**
	 * Open connection to ReceiverHelper
	 */
	public void open();

	/**
	 * Receive tuple from ReceiverHelper
	 * @return
	 * @throws EndOfResultsException
	 * @throws SNEEDataSourceException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SNEEException 
	 * @throws ReceiveTimeoutException 
	 */
	public Tuple receive()
	throws EndOfResultsException, SNEEDataSourceException, 
	TypeMappingException, SchemaMetadataException, SNEEException, 
	ReceiveTimeoutException;

	/**
	 * Close connection to ReceiverHelper
	 */
	public void close();

}
