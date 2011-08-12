package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import java.util.List;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public interface SourceReceiver {
	
	/**
	 * Open connection to ReceiverHelper
	 */
	public void open();

	/**
	 * Receive a list of tuples from ReceiverHelper
	 * @return a list of tuples
	 * @throws EndOfResultsException
	 * @throws SNEEDataSourceException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SNEEException 
	 * @throws ReceiveTimeoutException 
	 */
	public List<Tuple> receive()
	throws EndOfResultsException, SNEEDataSourceException, 
	TypeMappingException, SchemaMetadataException, SNEEException, 
	ReceiveTimeoutException;

	/**
	 * Close connection to ReceiverHelper
	 */
	public void close();

}
