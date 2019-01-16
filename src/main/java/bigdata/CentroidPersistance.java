package bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class CentroidPersistance {

	
	
	public static List<Centroid> load(Path path) throws JsonSyntaxException, IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
		Gson g = new Gson();
		String line;
		List<Centroid> ret = new ArrayList<Centroid>();
		while ((line = br.readLine())!= null) 
			ret.add(g.fromJson(line, Centroid.class));
		return ret;
	}
	
	public static void append(Centroid centroid, Path path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		OutputStream os = fs.append(path);
		PrintWriter pr = new PrintWriter(os);
		pr.println((new Gson()).toJson(centroid));
		pr.flush();
		os.close();
	}
	
	public static void dump(List<Centroid> centroids, Path path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		OutputStream os = fs.create(path);
		PrintWriter pr = new PrintWriter(os);
		Gson g = new Gson();
		for(Centroid c : centroids)
			pr.println(g.toJson(c));
		pr.flush();
		os.close();
	}
	
}
