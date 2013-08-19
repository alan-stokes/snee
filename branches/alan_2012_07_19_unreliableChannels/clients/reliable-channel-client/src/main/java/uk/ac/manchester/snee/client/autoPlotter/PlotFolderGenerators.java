package uk.ac.manchester.snee.client.autoPlotter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.snee.client.autoPlotter.Klevel.typeOfInput;

public class PlotFolderGenerators 
{
  static private HashMap<String, Query> queries = new HashMap<String, Query>();
  static private File tupleInputFile = null;
  static private File lifetimeInputFile = new File("/mnt/usb/1st1/edgeResults/data/lifetimeAndVarience");
  static private File outputFolder = null;
  static private File outputFolderRoot = new File("/mnt/usb/1st1/edgeResults/plots/");
  static private String sep = File.separator;
  static private File startupDataFolder = new File("/mnt/usb/1st1/edgeResults/data");
  
  
  public static void main(String [] args)
  {
    File [] typesOfNoise = startupDataFolder.listFiles();
    for(int index = 0; index < typesOfNoise.length; index++)
    {
      File typeOfNoise = typesOfNoise[index];
      if(typeOfNoise.isDirectory())
      {
        File [] distances = typeOfNoise.listFiles();
        for(int distanceIndex = 0; distanceIndex < distances.length; distanceIndex++)
        {
          tupleInputFile = distances[distanceIndex];
          String outputDistanceFolder = null;
          if(distances[distanceIndex].getName().equals("aggre0.2"))
            outputDistanceFolder = "8m";
          if(distances[distanceIndex].getName().equals("aggre0.4"))
            outputDistanceFolder = "16m";
          if(distances[distanceIndex].getName().equals("aggre0.6"))
            outputDistanceFolder = "24m";
          if(distances[distanceIndex].getName().equals("aggre0.8"))
            outputDistanceFolder = "32m";
          if(distances[distanceIndex].getName().equals("aggre1.0"))
            outputDistanceFolder = "40m";
            
          outputFolder = new File(outputFolderRoot.toString() + sep + typeOfNoise.getName() + sep + outputDistanceFolder);
          File queryTypeFolder = new File(outputFolderRoot.toString() + sep + typeOfNoise.getName());
          queryTypeFolder.mkdir();
          File distanceFolder = new File(queryTypeFolder.toString() + sep + outputDistanceFolder);
          distanceFolder.mkdir();
          startup(args, typeOfNoise.getName(), outputDistanceFolder);
        }
      }
    }
  }
  
  private static void runGnuplot(File specificOutputFolder,
                                 File mainDataFileDirectory,
                                 File DataOptDirectory1,
                                 File DataOptDirectory2,
                                 File DataOptDirectory3,
                                 File DataOptDirectory4,
                                 File mainDataFileDirectoryk3,
                                 String queryType,
                                 String outputName)throws IOException
  {
    String positionOfKey = null;
    if(queryType.equals("all") || queryType.equals("join"))
      positionOfKey = "set key right top";
    else
      positionOfKey = "set key left bottom";
    
    String gnuplotCommand = "dx=9. \n n=9 \n set style data linespoints \n " +
    "set samples 11, 11 \n" + positionOfKey + " \n " +
    "total_box_width_relative=0.3 \n gap_width_relative=0.03 \n " +
    "set boxwidth 0.033333333333 \n set xlabel \"lifetime in Completed Agenda Cycles (1000s)\" \n " +
    "set ylabel \"Percentage of tuples delivered to end user\" \n set xrange [0:110000] \n" +
    "set yrange [0:100] \n set xtic (\"0\" 0, \"10\" 10000, \"20\" 20000, \"30\" 30000, \"40\"" +
    " 40000, \"50\" 50000, \"60\" 60000, \"70\" 70000, \"80\" 80000 " +
    ", \"90\" 90000, \"100\" 100000, \"110\" 110000, \"120\" 120000) \n" +
    "set ytic (0,10,20,30,40,50,60,70,80,90,100) \n " +
    "plot \""+mainDataFileDirectory.getAbsolutePath() + "\" u 1:4 w linespoints lc rgb \"blue\" " +
    "pt 5 lt 3 ps 2 title \"K_e = 2 & K_n >= K_e\"," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 8:11 w linespoints lc rgb \"red\" " +
    "pt 5 lt 3 ps 2 title \"K_e = 1 & K_n >= K_e (optimistic)\", " +
    "\""+mainDataFileDirectory.getAbsolutePath() + "\" u 8:11:9:10 w xerrorbars lc rgb \"black\" " +
    "lt 1 pt 4 ps 2 notitle, \""+mainDataFileDirectory.getAbsolutePath() + "\" u 8:11:12:13 w " +
    "yerrorbars lc rgb \"black\" lt 1 pt 4 ps 2 notitle," +
    " \""+DataOptDirectory1.getAbsolutePath() + "\" u 1:2:3 w labels offset -2.5,+1 notitle," +
    " \""+DataOptDirectory2.getAbsolutePath() + "\" u 1:2:3 w labels offset -2.5,-1 notitle," +
    " \""+DataOptDirectory3.getAbsolutePath() + "\" u 1:2:3 w labels offset +2.5,-1 notitle," +
    " \""+DataOptDirectory4.getAbsolutePath() + "\" u 1:2:3 w labels offset +2.5,+1 notitle," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 22:25 w linespoints lc rgb" +
    " \"green\" pt 5 lt 3 ps 2 title \"k_e =1 && k_n = 1 (Static)\"" +
    ", \""+mainDataFileDirectory.getAbsolutePath() + "\" u 22:25:23:24 w xerrorbars lc rgb \"black\" " +
    "lt 1 pt 4 ps 2 notitle, \""+mainDataFileDirectory.getAbsolutePath() + "\" u 22:25:26:27 w " +
    "yerrorbars lc rgb \"black\" lt 1 pt 4 ps 2 notitle," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 22:25:28 w labels offset -2.5,-0.5 notitle," +
    " \""+mainDataFileDirectoryk3.getAbsolutePath() + "\" u 1:4 w linespoints lc rgb \"orange\" " +
    "pt 5 lt 3 ps 2 title \"K_e = 3 & K_n >= K_e\", \""+mainDataFileDirectoryk3.getAbsolutePath() + "\"" +
    " u 1:4:3:2 w xerrorbars lc rgb \"black\" lt 1 pt 4 ps 2 notitle," +
    " \""+mainDataFileDirectoryk3.getAbsolutePath() + "\" u 1:4:5:6 w yerrorbars lc rgb \"black\" " +
    "lt 1 pt 4 ps 2 notitle, \""+mainDataFileDirectoryk3.getAbsolutePath() + "\" u 1:4:7 w labels offset" +
    " -2.5,-0.5 notitle, \""+mainDataFileDirectory.getAbsolutePath() + "\" u 15:18 w linespoints lc rgb" +
    " \"brown\" pt 5 lt 3 ps 1 title \"K_e = K_n (Pessimistic)\", " +
    "\""+mainDataFileDirectory.getAbsolutePath() + "\" u 15:18:16:17 w xerrorbars lc rgb \"black\" " +
    "lt 1 pt 4 ps 1 notitle, \""+mainDataFileDirectory.getAbsolutePath() + "\" u 15:18:19:20 w " +
    "yerrorbars lc rgb \"black\" lt 1 pt 4 ps 1 notitle," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 15:18:21 w labels offset -2.5,-0.5 notitle," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 1:4:3:2 w xerrorbars lc rgb \"black\" lt" +
    " 1 pt 4 ps 2 title \"variance from different topologies\"," +
    " \""+mainDataFileDirectory.getAbsolutePath() + "\" u 1:4:5:6 w yerrorbars lc rgb \"black\" " +
    "lt 1 pt 4 ps 2 notitle, \""+mainDataFileDirectory.getAbsolutePath() + "\" u 1:4:7 w labels" +
    " offset -2.5,-0.5 notitle \n set term png dash size 1024,768" +
    "\n set output \""+specificOutputFolder.toString() + sep + "" + outputName + ".png\" \n replot \n";

    BufferedWriter out = new BufferedWriter(
        new FileWriter(new File(specificOutputFolder.toString() + sep + "gnuplotScript")));
    out.write(gnuplotCommand);
    System.out.println(gnuplotCommand);
    out.flush();
    out.close();
    
    String[] command = {"gnuplot", "-e", "load \"" + specificOutputFolder.toString() + sep + "gnuplotScript" + "\"" + "\n exit"};
    try {
      Runtime rt = Runtime.getRuntime();
      Process proc = rt.exec(command);
  } catch (Exception e) {
      System.err.println("Fail: " + e);
  }
  }

  private static void startup(String [] args, String typeOfNoise, String Distance)
  {
    try
    {
      BufferedReader in;
      if(args.length ==2 )
        in = new BufferedReader(new FileReader(new File(args[0])));
      else
        in = new BufferedReader(new FileReader(tupleInputFile));
      
      String line = "";
      String queryID = "";
      while((line = in.readLine())!= null)
      {
        String [] bits = line.split(" ");
        if(bits.length !=1 && bits.length !=0)
        {
          updateDataStore(line, queryID, typeOfInput.TUPLES);
        }
        else
        {
          if(line.equals(""))
            queryID = "all";
          else
          {
            queryID = line.split(" ")[0];
          } 
        }
      }
      if(args.length ==2 )
        in = new BufferedReader(new FileReader(new File(args[1])));
      else
        in = new BufferedReader(new FileReader(lifetimeInputFile));
      line = "";
      queryID = "";
      while((line = in.readLine())!= null)
      {
        String [] bits = line.split(" ");
        if(bits.length !=1)
        {
          updateDataStore(line, queryID, typeOfInput.LIFETIME);
        }
        else
        {
          queryID = line;
        }
      }
      
      outputDataFilesForPlotter(typeOfNoise, Distance);
      
    }
    catch (FileNotFoundException e)
    {
      System.out.println("file with name " + tupleInputFile + " was not found");
      e.printStackTrace();
    }
    catch (IOException e)
    {
      System.out.println("file with name " + tupleInputFile + " was not readable");
      e.printStackTrace();
    }
  }
  
  
  
  private static void outputDataFilesForPlotter(String typeOfNoise, String distance) throws IOException
  {
    Iterator<String> typesOfQuery = queries.keySet().iterator();
    while(typesOfQuery.hasNext())
    {
      String queryType = typesOfQuery.next();
      File specificOutputFolder = new File(outputFolder.toString()+ sep + queryType);
      specificOutputFolder.mkdir();
      BufferedWriter mainOut = 
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "MainData")));
      BufferedWriter k3out =  
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "MainDatak3")));
      BufferedWriter opt1out =  
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "DataOpt1")));
      BufferedWriter opt2out =  
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "DataOpt2")));
      BufferedWriter opt3out =  
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "DataOpt3")));
      BufferedWriter opt4out =  
        new BufferedWriter(new FileWriter(
            new File(specificOutputFolder.toString() + sep + "DataOpt4")));
      
      Query q = queries.get(queryType);
      HashMap<Integer, Klevel> data = q.getData();
      for(int kn = 2; kn <= 5; kn++)
      {
        String output = "";
        // ke =2 kn > ke
        output = output.concat(data.get(kn).getKe2Lifetime().toString() + " ");
        output = output.concat(data.get(kn).getMinKe2Lifetime().toString() + " ");
        output = output.concat(data.get(kn).getMaxKe2Lifetime().toString() + " ");
        output = output.concat(data.get(2).getTuplePercentage().toString() + " ");
        output = output.concat(data.get(2).getTuplePercentageMin().toString() + " ");
        output = output.concat(data.get(2).getTuplePercentageMax().toString() + " ");
        output = output.concat(kn + ",2" + " ");
        // ke = 1 kn > ke (optistic)
        output = output.concat(data.get(kn).getOptimsiticLifetime().toString() + " ");
        output = output.concat(data.get(kn).getMinOptimsiticLifetime().toString() + " ");
        output = output.concat(data.get(kn).getMaxOptimsiticLifetime().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentage().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentageMin().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentageMax().toString() + " ");
        output = output.concat(kn + ",1" + " ");
        // ke = kn (pessmistic)
        output = output.concat(data.get(kn).getPessimisticLifetime().toString() + " ");
        output = output.concat(data.get(kn).getMinPessimisticLifetime().toString() + " ");
        output = output.concat(data.get(kn).getMaxPessimisticLifetime().toString() + " ");
        output = output.concat(data.get(kn).getTuplePercentage().toString() + " ");
        output = output.concat(data.get(kn).getTuplePercentageMin().toString() + " ");
        output = output.concat(data.get(kn).getTuplePercentageMax().toString() + " ");
        output = output.concat(kn + ","+  kn + " ");
        // ke = 1 kn = 1 (static)_
        output = output.concat(data.get(1).getStaticLifetime().toString() + " ");
        output = output.concat(data.get(1).getMinStaticLifetime().toString() + " ");
        output = output.concat(data.get(1).getMaxStaticLifetime().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentage().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentageMin().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentageMax().toString() + " ");
        output = output.concat("1,1" + " ");
        mainOut.write(output + " \n");
        output = "";
         // ke = 3 kn > ke 
        if(kn >= 3)
        {
          output = output.concat(data.get(kn).getKe3Lifetime().toString() + " ");
          output = output.concat(data.get(kn).getMinKe3Lifetime().toString() + " ");
          output = output.concat(data.get(kn).getMaxKe3Lifetime().toString() + " ");
          output = output.concat(data.get(3).getTuplePercentage().toString() + " ");
          output = output.concat(data.get(3).getTuplePercentageMin().toString() + " ");
          output = output.concat(data.get(3).getTuplePercentageMax().toString() + " ");
          output = output.concat(kn + ",3" + " ");
          k3out.write(output + " \n");
        }
        output = "";
        output = output.concat(data.get(kn).getOptimsiticLifetime().toString() + " ");
        output = output.concat(data.get(1).getTuplePercentage().toString() + " ");
        output = output.concat(kn + ",1" + " ");
        if(kn == 2)
          opt1out.write(output + " \n");
        if(kn ==3)
          opt2out.write(output + " \n");
        if(kn ==4)
          opt3out.write(output + " \n");
        if(kn ==5)
          opt4out.write(output + " \n");
      }
      
      mainOut.flush();
      k3out.flush();
      opt1out.flush();
      opt2out.flush();
      opt3out.flush();
      opt4out.flush();
      mainOut.close();
      k3out.close();
      opt1out.close();
      opt2out.close();
      opt3out.close();
      opt4out.close();
      
      String outputName = typeOfNoise + distance + queryType;
      runGnuplot(specificOutputFolder, 
                 new File(specificOutputFolder.toString() + sep + "MainData"),
                 new File(specificOutputFolder.toString() + sep + "DataOpt1"),
                 new File(specificOutputFolder.toString() + sep + "DataOpt2"),
                 new File(specificOutputFolder.toString() + sep + "DataOpt3"),
                 new File(specificOutputFolder.toString() + sep + "DataOpt4"),
                 new File(specificOutputFolder.toString() + sep + "MainDatak3"),
                 queryType,
                 outputName);
    }
  }

  /**
   * places data within line into correct data store
   * @param line
   * @param queryID
   */
  private static void updateDataStore(String line, String queryID, typeOfInput typeofInput)
  {
    if(typeofInput.toString().equals("LIFETIME") && line.split(" ")[0].equals("2"))
      System.out.println();
    
    if(queries.get(queryID) == null)
    {
      HashMap<Integer, Klevel> data = new HashMap<Integer, Klevel>();
      String [] bits = line.split(" ");
      Klevel newK = new Klevel(line, typeofInput);
      data.put(Integer.parseInt(bits[0]), newK);
      Query q = new Query(data);
      queries.put(queryID, q);
    }
    else
    {
      Query q = queries.get(queryID);
      HashMap<Integer, Klevel> data = q.getData();
      String [] bits = line.split(" ");
      Klevel newK = null;
      if(data.get(Integer.parseInt(bits[0])) == null)
        newK = new Klevel(line, typeofInput);
      else
      {
        newK = data.get(Integer.parseInt(bits[0])); 
      }
      newK.addData(line, typeofInput);
      data.put(Integer.parseInt(bits[0]), newK);
    }
    if(typeofInput.toString().equals("LIFETIME"))
    {
      String [] bits = line.split(" ");
      Query q = queries.get(queryID);
      HashMap<Integer, Klevel> data = q.getData();
      Klevel newK = data.get(1);
      newK.addStaticData(bits[13], bits[14], bits[15]);
      data.put(1, newK);
    }
  }
}
