package uk.ac.manchester.cs.snee.sncb;

import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class TinyOS_SNCB_Utils {

	protected static void printTelosBCommands(String queryOutputDir, 
			SensorNetworkQueryPlan qep,
			String targetDirName, String serialPort) {
		String nescOutputDir = System.getProperty("user.dir") + "/"
		+ queryOutputDir + targetDirName;
		System.out.println("(1) Upload executables to each mote via the USB " +
			"cable. For example, for mote one this is done as follows:");
		
		Iterator<Site> siteIter = qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
			Site s = siteIter.next();
			String id = s.getID();
			System.out.println("*** PLUG IN MOTE "+id+" ***");
			System.out.println("    cd "+nescOutputDir+"/mote"+id);
			System.out.println("    make telosb install,"+id+"\n");			
		}

		System.out.println("(2a) To view raw packets:");
		System.out.println("    java net.tinyos.tools.Listen -comm serial@"+
				serialPort+":telos\n");		
		System.out.println("(2b) To view tuples:");
		System.out.println("    mig java -target=null -java-classname="+
				"DeliverMessage QueryPlan.h DeliverMessage " +
				"-o DeliverMessage.java");
		System.out.println("    javac DeliverMessage.java");
		System.out.println("    java net.tinyos.tools.MsgReader -comm serial@"+
				serialPort+":telos DeliverMessage\n");
		
		System.out.println("(3) Start the query plan by pressing reset button "+
				"on all motes simultaneously.");
		

	}

	protected static void printTossimCommands(String queryOutputDir, String targetDirName) {
		String nescOutputDir = System.getProperty("user.dir") + "/"
		+ queryOutputDir + targetDirName;
		System.out.println("cd "+nescOutputDir);
		System.out.println("chmod a+rx *");
		System.out.println("./runTossim.py");
	}
	
	protected static void printAvroraCommands(String queryOutputDir, 
			SensorNetworkQueryPlan qep, String targetDirName,
			CodeGenTarget target) {
		String nescOutputDir = System.getProperty("user.dir") + "/"
		+ queryOutputDir + targetDirName;
		
		int gatewayID = qep.getGateway();
		int maxSiteID = qep.getRT().getMaxSiteID();
		String platform = "mica2";
		StringBuffer sensorData = new StringBuffer();
		StringBuffer nodeCount = new StringBuffer();
		StringBuffer elfString= new StringBuffer();

		if (target==CodeGenTarget.AVRORA_MICAZ_T2) {
			platform = "micaz";
		}
		
		RT rt = qep.getRT();
		for (int i=0; i<=maxSiteID; i++) {
			String siteID = ""+i;
			Site site = rt.getSite(siteID);
			if (site!=null) {
				if (site.isSource()) {
					if (sensorData.length()==0) {
						sensorData.append("-sensor-data=light:"+siteID+":.");
					} else {
						sensorData.append(",light:"+siteID+":.");
					}
				}
				elfString.append("mote"+siteID+".elf ");
			} else {
				elfString.append("Blink.elf ");
			}
			
			if (nodeCount.length()==0) {
				nodeCount.append("-nodecount=1");
			} else {
				nodeCount.append(",1");
			}
			
		}
		
		System.out.println("*** To start Avrora ***");
		System.out.println("cd "+nescOutputDir);
		System.out.println("java avrora.Main -mcu=mts300 -platform="+platform+" " +
				"-simulation=sensor-network -colors=false -seconds=100 " +
				"-monitors=packet,serial -ports="+gatewayID+":0:2390 -random-seed=1 " +
				sensorData + " " + "-report-seconds "+nodeCount+" "+elfString+" \n");
		
		System.out.println("*** In a separate terminal window ***");
		System.out.println("(2a) To view raw packets:");
		System.out.println("    java net.tinyos.tools.Listen");		
		System.out.println("(2b) To view tuples:");
		System.out.println("    cd "+nescOutputDir+"/mote"+gatewayID);
		System.out.println("    mig java -target=null -java-classname="+
				"DeliverMessage QueryPlan.h DeliverMessage " +
				"-o DeliverMessage.java");
		System.out.println("    javac DeliverMessage.java");
		System.out.println("    java net.tinyos.tools.MsgReader DeliverMessage\n");
	}

	
}
