package bigdata;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;

import org.apache.hadoop.io.SequenceFile.Reader;

import bigdata.util.FileFormatConverter;
import bigdata.util.LineParser;

public class Main {

	static String inputPath;
	static String outputPath;
	static int k;
	static ArrayList<Integer> columns;

	public static void main(String[] args) throws Exception {

		parseArguments(args);

		final Path sequenceInputPath = FileFormatConverter.textToSequencial(inputPath, columns);
		// TODO: remove /part-m-00000 ?
		initCentroidFile(new Path(sequenceInputPath.toString() + "/part-m-00000"), k);

		while (true) {

			KMeans.startIteration(columns);

			// check stop condition
			List<Centroid> currentCentroids = CentroidPersistance.load(new Path("current_centroids.json"));
			List<Centroid> newCentroids = CentroidPersistance.load(new Path("new_centroids.json"));
			boolean stop = CentroidComparator.compare(currentCentroids, newCentroids);

			// remove temporary files
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path("iteration_output"), true);
			fs.delete(new Path("current_centroids.json"), true);
			fs.rename(new Path("new_centroids.json"), new Path("current_centroids.json"));

			if (stop)
				break;
		}
		KMeans.finalIteration(columns, new Path(outputPath));
		clean();
		System.exit(0);
	}

	private static void initCentroidFile(Path sequenceFilePath, int nbCentroids)
			throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Reader reader = new Reader(fs, sequenceFilePath, conf);
		NullWritable key = NullWritable.get();
		Text val = new Text(); // also could be wrong

		int idGen = 0;
		ArrayList<Centroid> centroids = new ArrayList<Centroid>();
		while (reader.next(key, val)) {
			Centroid newCentroid = new Centroid(LineParser.parsePoint(val.toString(), columns), ++idGen);
			centroids.add(newCentroid);
			if (--nbCentroids == 0)
				break;
		}
		CentroidPersistance.dump(centroids, new Path("current_centroids.json"));
	}

	private static void parseArguments(String[] args) throws Exception {
		if (args.length < 4)
			throw new Exception("unvalid number of arguments");
		inputPath = args[0];
		outputPath = args[1];
		k = Integer.parseInt(args[2]);
		columns = new ArrayList<Integer>();
		for (int i = 3; i < args.length; i++)
			columns.add(new Integer(args[i]));
	}

	private static void clean() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("sequence_input"), true);
		fs.delete(new Path("current_centroids.json"), false);
	}
}
