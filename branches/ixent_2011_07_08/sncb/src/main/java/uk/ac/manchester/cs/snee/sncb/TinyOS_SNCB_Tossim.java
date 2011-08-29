package uk.ac.manchester.cs.snee.sncb;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;


public class TinyOS_SNCB_Tossim extends TinyOS_SNCB implements SNCB 
{
  
  private String queryOutputDir;

  public TinyOS_SNCB_Tossim(double duration) throws SNCBException 
  {
    super(duration);
	this.target = CodeGenTarget.TOSSIM_T2;
  }

	public TinyOS_SNCB_Tossim() throws SNCBException 
	{
	    this(Double.POSITIVE_INFINITY);
	}

	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		//do nothing!
	}
  
  public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
	      String queryOutputDir, MetadataManager costParams)
  throws SNCBException {
	  super.register(qep, queryOutputDir, costParams);
	  this.queryOutputDir = queryOutputDir;
	  System.out.println("Not using node controller, or no mote "+
	            "plugged in, so unable to send query plan using" +
	            "Over-the-air Programmer. ");
	        	System.out.println("Please proceed using manual commands.\n");
	  TinyOS_SNCB_Utils.printTossimCommands(queryOutputDir,
				this.getTargetDirName());
	  System.exit(0);  	
	  return null;
  }

  @Override
  public void start() throws SNCBException {
		throw new SNCBException("Start not supported by Tossim SNCB");
  }

	@Override
	public void stop(SensorNetworkQueryPlan qep) throws SNCBException {
		throw new SNCBException("Stop not supported by Tossim SNCB");	
	}
  
	@Override
	public void deregister(SensorNetworkQueryPlan qep) throws SNCBException {
		throw new SNCBException("Deregister not supported by Tossim SNCB");

	}
}
