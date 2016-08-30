import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import weka.clusterers.ClusterEvaluation;
import weka.clusterers.MakeDensityBasedClusterer;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

/**
 * 
 */

/**
 * @author Tirthraj
 *
 */
public class DensityBasedClustering {
	
	File dataFile;
	File idFile;
	int noOfClusters;
	File outputFile;
	
	public DensityBasedClustering(File dataFile, File idFile, int noOfClusters, File outputFile) {
		super();
		this.dataFile = dataFile;
		this.idFile = idFile;
		this.noOfClusters = noOfClusters;
		this.outputFile = outputFile;
	}
	
	/**
	 * Implements Density Based Clustering on given data file,
	 * and maps results to their respective ids
	 * 
	 * @author Tirthraj
	 * @throws Exception 
	 */
	public void implementClustering() throws Exception {
		
		// load arff data for clustering
		System.out.println("Loading data");
		long startTime = System.currentTimeMillis();
		
		Instances data = new Instances(
								new BufferedReader(
										new FileReader(this.dataFile)));
		
		long endTime = System.currentTimeMillis();
		System.out.println("\tLoading Time: "+(endTime-startTime)/60000);
		
		// build clusterer
		System.out.println("Building cluster");
		startTime = System.currentTimeMillis();
		
		MakeDensityBasedClusterer mdbCluster = new MakeDensityBasedClusterer();
		mdbCluster.setNumClusters(noOfClusters);
		mdbCluster.buildClusterer(data);
		
		endTime = System.currentTimeMillis();
		System.out.println("\tBuilding Time: "+(endTime-startTime)/60000);
		
		// perform cluster evaluation
		System.out.println("Evaluating cluster");
		startTime = System.currentTimeMillis();
		
		ClusterEvaluation cEval = new ClusterEvaluation();
		cEval.setClusterer(mdbCluster);
		cEval.evaluateClusterer(data);
		
		endTime = System.currentTimeMillis();
		System.out.println("\tEvaluation Time: "+(endTime-startTime)/60000);
		
		System.out.println("Initiated output creation");
		startTime = System.currentTimeMillis();
		
		// array of cluster assignments for each document in given order
		double[] assignments = cEval.getClusterAssignments();
		
		// get document id in given sequence
		FileReader fReader = new FileReader(this.idFile);
		BufferedReader bReader = new BufferedReader(fReader);
		
		// prepare output writer
		FileWriter fWriter = new FileWriter(this.outputFile);
		
		int index = 0;
		String line = "";
		
		while ((line = bReader.readLine()) != null) {
			line = line.trim();
			String outputLine = line + " " + assignments[index] + "\n";
			
			fWriter.write(outputLine);
			System.out.print(".");
			index++;
		}
		
		endTime = System.currentTimeMillis();
		System.out.println("\n\tOutput Processing Time: "+(endTime-startTime)/60000);
		
		bReader.close();
		fReader.close();
		fWriter.close();
	}
}
