/**
 * 
 */
package uk.ac.manchester.cs.snee.operators.evaluator.receivers;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.data.generator.TupleGenerator;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

/**
 * @author Praveen
 * 
 */
public class DummyUDPStreamReceiver implements SourceReceiver {

	private TupleGenerator tupleGenerator;
	private String streamName;
	private ExtentMetadata stream;
	private double rate;
	private int curIndex = 0;
	private int counter = 0;
	/**
	 * The list of streams
	 */
	private List<ExtentMetadata> _streams;

	public DummyUDPStreamReceiver(ReceiveOperator receiveOp, SourceMetadataAbstract source) {
		MetadataManager schema;
		try {
			schema = new MetadataManager(null);

			_streams = schema.getPushedExtents();
			
			streamName = receiveOp.getExtentName();
			
			for (ExtentMetadata loopstream : _streams) {
				String curStreamName = loopstream.getExtentName().toLowerCase();
				if (curStreamName.equalsIgnoreCase(streamName)) {
					stream = loopstream;
					tupleGenerator = new TupleGenerator(stream);
					break;
				}
			}
			
			rate = stream.getRate();

			System.out.println("Initiating receive dummy operator for stream: "+streamName+"with rate: "+rate);
			counter = 1;

			if (rate > 10) {
				counter = (int) rate;
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (TypeMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (SchemaMetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (SNEEConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (MetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (UnsupportedAttributeTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (SourceMetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (TopologyReaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (SNEEDataSourceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (CostParametersException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (SNCBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Tuple> receive() throws EndOfResultsException,
			SNEEDataSourceException, TypeMappingException,
			SchemaMetadataException, SNEEException, ReceiveTimeoutException {
		List<Tuple> tupleList = new ArrayList<Tuple>(counter);

		for (int i = 0; i < counter; i++) {
			curIndex++;
			Tuple tuple = tupleGenerator.generateTuple(curIndex);
			//System.out.println(streamName+"__"+tuple);
			tupleList.add(tuple);
		}
		//System.out.println("Returning Tuple lst");
		return tupleList;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
