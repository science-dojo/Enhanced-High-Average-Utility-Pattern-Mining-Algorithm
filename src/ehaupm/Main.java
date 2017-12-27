package ehaupm;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

/**
 * 每个node附带的list都有四个字段
 * @author RSF
 *
 */
public class Main {
	
	public static void main(String[] args) throws Exception{
		String folder = args[0]; // specify the folder containing quantityDB and its profit table.
		String fileName = args[1]; // quantityDB name
		String quantityDBPath = folder+fileName+".txt"; // unique quantityDB address
		String profitPath = folder+fileName +"_UtilityTable.txt"; // unique profit table address
		double delta = Double.parseDouble(args[2]); // specify the minimum average utility threshold.
        String HAUIsPath = args[3]; // specify the file storing the discovered HAUIs. If not specify, enter `null`
        String statusPath = args[4]; // specify the file storing the running status of EHAUPM. If not specify, enter `null`

        long startTimestamp = System.currentTimeMillis();
        EHAUPM ehaupm = new EHAUPM();
		ehaupm.runAlgorithm(HAUIsPath, profitPath, quantityDBPath, delta);
        long endTimestamp = System.currentTimeMillis();

        BufferedWriter statusWriter = null;
        if(statusPath==null || statusPath.equalsIgnoreCase("null")){
            statusWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        }else {
            statusWriter = new BufferedWriter(new FileWriter(statusPath));
        }

        statusWriter.write(String.format("Total_time(s): %.2f\n", (endTimestamp - startTimestamp) / (double)1000));
        statusWriter.write(String.format("Memory_usage:%.2f\n", ehaupm.memRecorder.maxConsumationMemory));
        statusWriter.write("Haui_count:"+ ehaupm.nhauis +"\n");
        statusWriter.write("candidate_count:"+ ehaupm.joinCount +"\n");


//		local_exe();
	}
	
	static void local_exe() throws Exception {
		String location="E:\\Data_mining\\dataset\\sort_ascend_item_DB\\";
		 String file="kosarak";
		        file="chess";
//		        file="retail";
		        file="accidents";
//                file="mushroom";
		 String quantityDBPath = location+file+".txt";
		 String profitPath=location+file+"_UtilityTable.txt";
//		 double support=1.4e-5;
//		 double minAU=2877986820.0*support;
//		 int minAU=8000000;
//		 	 minAU=0;
//		 	 minAU=9000;
////		 	 minAU=100000000;

		 //String output = "E:\\Data_mining\\dataset\\sort_ascend_item_DB\\IHAUI_output\\test\\"+file+"_"+support+".txt";
		// Applying the HUIMiner algorithm

         long startTimestamp = System.currentTimeMillis();

		 EHAUPM ehaupm = new EHAUPM();
		 ehaupm.runAlgorithm(null, profitPath, quantityDBPath, 0.0035);

		 long endTimestamp = System.currentTimeMillis();

		 //
        System.out.printf("Total_time(s): %.2f\n", (endTimestamp - startTimestamp) / (double)1000);
        System.out.printf("Memory:%.3f\n", ehaupm.memRecorder.maxConsumationMemory );
        System.out.println("Haui_count:"+ ehaupm.nhauis);
        System.out.println("Join_count:"+ ehaupm.joinCount);
	}
	
}
