package bigdata;

import java.util.List;

public class CentroidComparator {

	private static int curr = 1;

	public static boolean compare(List<Centroid> currentCentroids, List<Centroid> newCentroids) throws Exception {
		// TODO: comparison crytiria 
		if (currentCentroids.size() != newCentroids.size()) {
			System.out.println("current : " + currentCentroids.size());
			System.out.println("new: " + newCentroids.size());
			throw new Exception("implementation error!!");
		}
			
		
		if (curr++ == 3)
			return true; // stop after three loops
		return false;
	}

}
