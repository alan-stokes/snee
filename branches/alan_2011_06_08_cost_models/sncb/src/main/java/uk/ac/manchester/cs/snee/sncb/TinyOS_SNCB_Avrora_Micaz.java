package uk.ac.manchester.cs.snee.sncb;

import java.io.BufferedInputStream;
import java.io.IOException;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;

public class TinyOS_SNCB_Avrora_Micaz extends TinyOS_SNCB implements SNCB 
{
 /**
	 * 
	 */
	private static final long serialVersionUID = -9041566416605999709L;
private Process avrora = null;
 
 public TinyOS_SNCB_Avrora_Micaz(double duration)throws SNCBException 
 {
	 super(duration);
	 this.target = CodeGenTarget.AVRORA_MICAZ_T2;
	 if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CONTROL_RADIO)) {
			try {
				this.controlRadio = SNEEProperties
					.getBoolSetting(SNEEPropertyNames.SNCB_CONTROL_RADIO);
			} catch (SNEEConfigurationException e) {
				this.controlRadio = false;
			}
	 }
 }
 
 
	public TinyOS_SNCB_Avrora_Micaz() throws SNCBException 
	{
	   this(Double.POSITIVE_INFINITY);
	}

	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		//do nothing!
	}
 
	public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager costParams)
	throws SNCBException 
	{
	  super.register(qep, queryOutputDir, costParams);
	  try 
	  {
		String avroraCommand = TinyOS_SNCB_Utils.printAvroraCommands(queryOutputDir, qep, 
		                       this.getTargetDirName(), this.target);
		System.out.println(avroraCommand);
		 	
	    // String nescOutputDir = System.getProperty("user.dir") + "/"
	    // + queryOutputDir + getTargetDirName();
	        
	    Runtime rt = Runtime.getRuntime();
	    avrora = rt.exec("java avrora.Main " + avroraCommand);
	    System.out.println("waiting for avrora to initialise");
	    BufferedInputStream avroraReader = new BufferedInputStream(avrora.getInputStream());
	    boolean success = waitForAvrora(avroraReader);
	    if(success)
	    {
	      mr = setUpResultCollector(qep, queryOutputDir);
	      if (logger.isDebugEnabled())
	          logger.debug("RETURN register()");
	        return mr;
	    }
	    else
	    {
	      if(avrora != null)
	        avrora.destroy();
	      throw new SNCBException("avrora failed to get ready");
	    }
      } 
	  catch (Exception e) 
	  {
        if(avrora != null)
          avrora.destroy();
        e.printStackTrace();
        logger.warn(e.getLocalizedMessage(), e);
        throw new SNCBException(e.getLocalizedMessage(), e);
      }
	}
 
  private boolean waitForAvrora(BufferedInputStream avroraReader) throws IOException, InterruptedException 
  { 
    String outputString = "";
	boolean found = false;
    while(!found)
    {
      byte [] output;
      output = new byte[avroraReader.available()];
      avroraReader.read(output);
      String currentOutputString = new String(output);
      System.out.println(currentOutputString);
      outputString = outputString.concat(currentOutputString);
      String test = "Waiting for serial connection on port 2390...";
      if(outputString.contains(test))
        found = true;
	  else 
	  {
	    if(currentOutputString.equals(""))
		{
		  try
		  {
		    avrora.exitValue();
			return false;
		  }
		  catch(IllegalThreadStateException e)
		  {
			Thread.currentThread();
			Thread.sleep(5000);
			return waitForAvrora(avroraReader);
		  }
		}
		Thread.currentThread();
		Thread.sleep(5000);
	  }
    }
    System.out.println("avrora ready");
    avroraReader.close();
    return true;
  }


  public void start() throws SNCBException
  {
   
  }
	
  public void stop(SensorNetworkQueryPlan qep) throws SNCBException 
  {
    isStarted = false;
    avrora.destroy();
  }
 
  public void deregister(SensorNetworkQueryPlan qep) throws SNCBException 
  {
    if (logger.isDebugEnabled())
     logger.debug("ENTER deregister()");
    if (logger.isDebugEnabled())
     logger.debug("RETURN deregister()");
  }

  // @Override
  public void waitForQueryEnd() throws InterruptedException
  {
    avrora.waitFor();
  }


  @Override
  public void setOutputFolder(String newTargetDir) 
  {
		
  }
}