package uk.ac.manchester.cs.snee.sncb;

import java.io.BufferedInputStream;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;

public class TinyOS_SNCB_Avrora_Mica2 extends TinyOS_SNCB implements SNCB 
{
 /**
	 * 
	 */
	private static final long serialVersionUID = 1505085596886747885L;
private Process avrora = null;
 
 public TinyOS_SNCB_Avrora_Mica2(double duration) throws SNCBException 
 {
	 super(duration);
	 this.target = CodeGenTarget.AVRORA_MICA2_T2;
	 if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CONTROL_RADIO)) {
			try {
				this.controlRadio = SNEEProperties
					.getBoolSetting(SNEEPropertyNames.SNCB_CONTROL_RADIO);
			} catch (SNEEConfigurationException e) {
				this.controlRadio = false;
			}
	 }
 }
 
 
	public TinyOS_SNCB_Avrora_Mica2() throws SNCBException 
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
	 	try {
		 	String avroraCommand = TinyOS_SNCB_Utils.printAvroraCommands(queryOutputDir, qep, 
		              this.getTargetDirName(), this.target);
		 	System.out.println(avroraCommand);
		 	
	     //   String nescOutputDir = System.getProperty("user.dir") + "/"
	       // + queryOutputDir + getTargetDirName();
	        
	        Runtime rt = Runtime.getRuntime();
	        avrora = rt.exec("java avrora.Main " + avroraCommand);
	        BufferedInputStream avroraReader = new BufferedInputStream(avrora.getInputStream());
	        String outputString = "";
	        System.out.println("waiting for avrora to initialise");
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
			else {
				Thread.currentThread();
				Thread.sleep(5000);
			}
	        }
	        System.out.println("avrora ready");
	        avroraReader.close();
	        mr = setUpResultCollector(qep, queryOutputDir);
    } catch (Exception e) {
        if(avrora != null)
          avrora.destroy();
        e.printStackTrace();
        logger.warn(e.getLocalizedMessage(), e);
        throw new SNCBException(e.getLocalizedMessage(), e);
      }
      if (logger.isDebugEnabled())
        logger.debug("RETURN register()");
      return mr;
	}
 
	 
 public void start() throws SNCBException
 {
   
 }
	
 public void stop(SensorNetworkQueryPlan qep) throws SNCBException 
 {
   isStarted = false;
   avrora.destroy();
 }
 
 public void deregister(SensorNetworkQueryPlan qep) throws SNCBException {
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
public void setOutputFolder(String newTargetDir) {
	// TODO Auto-generated method stub
	
}
 
}