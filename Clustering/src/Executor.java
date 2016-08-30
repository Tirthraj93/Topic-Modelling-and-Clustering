import java.io.File;

/**
 * 
 */

/**
 * @author Tirthraj
 *
 */
public class Executor {

	private static String DATA_PATH = 
			"D:\\NEU\\Sem 2 - Summer\\IR\\Assignment\\CS6200_Tirthraj_Parmar\\"
										+ "Clustering and Topic Models\\Data\\";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		long startTime = System.currentTimeMillis();
		
		File dataFile = new File(DATA_PATH + "data.arff");
		File idFile = new File(DATA_PATH + "doc_ids");
		int noOfClusters = 25;
		File outputFile = new File(DATA_PATH + "clustering_output");
		
		DensityBasedClustering dbClustering = 
				new DensityBasedClustering(dataFile, idFile, noOfClusters, outputFile);
		
		try {
			dbClustering.implementClustering();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("\n\nTotal Execution Time: "+(endTime-startTime)/60000);
	}

}
