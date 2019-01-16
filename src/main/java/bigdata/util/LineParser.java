package bigdata.util;

import java.util.ArrayList;
import java.util.List;

import bigdata.PointAcc;

public class LineParser {

	public static boolean verifLine(String line, ArrayList<Integer> columnIndexes) {
		String[] parse = line.split(",");
		try {
			for(int index : columnIndexes) 
				Double.parseDouble(parse[index]);
			return true;
		}catch(Exception e ) {
			return false;
		}
	}
	
	public static PointAcc parsePoint(String line, List<Integer> columnIndexes) {
		String[] parse = line.split(",");
		ArrayList<Double> coordinates = new ArrayList<Double>();
		for(int i : columnIndexes) 
			coordinates.add(Double.parseDouble(parse[i]));
		return new PointAcc(coordinates);
	}
}
