package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import org.graphframes.lib.PageRank$;
import scala.Option;
import scala.Serializable;
import scala.Some;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxesRunTime;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_4 {

	public static double calculate_sqr_deltas(double[] matrixA, double[] matrixB){
		double sum_sqr = 0;
		for (int i =0; i < matrixA.length; i ++){
			sum_sqr += Math.pow(matrixA[i] - matrixB[i],2);
		}
		return Math.pow(sum_sqr,0.5);
	}

	public static double[] asDoubleArray(List<Double> o_array){
		double[] v = new double[o_array.size()];
		int i = 0;
		for(double num : o_array ) {
			v[i++] = num;
		}
		return v;
	}

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws Exception{

		String  path = "G:\\Documentos\\MasterDegree\\BDMA\\Classes\\UPC\\SDM\\Lab2\\SparkGraphXassignment\\src\\main\\resources\\";

		//Vertex Creation

		java.util.List<Row> vertices_list = new ArrayList<Row>();

		File file = new File(path + "wiki-vertices.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			String[] columns = line.split("\t", -1);
			vertices_list.add(RowFactory.create(columns[0], columns[1]));
		}
		br.close();

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		//Edges Creation

		java.util.List<Row> edges_list = new ArrayList<Row>();

		file = new File(path + "wiki-edges.txt");
		br = new BufferedReader(new FileReader(file));
		while ((line = br.readLine()) != null) {
			String[] columns = line.split("\t", -1);
			edges_list.add(RowFactory.create(columns[0], columns[1], "links"));
		}
		br.close();

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("relationship", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges).unpersist().cache();

		Dataset<Row> motifs = gf.edges().groupBy("src").count();
		motifs.select("count").groupBy().max("count").show();


		long num_vertex = gf.vertices().groupBy().count().as(Encoders.LONG()).collectAsList().get(0);

		System.out.println(num_vertex);

		int numIterations = 10;
		int skip = 5;
		double[] reserProb_toTest = {0.0001,0.001,0.1,0.15,0.2,0.3,0.5};

		//double[] reserProb_toTest = {0.0001};

		long [][] times = new long[numIterations][reserProb_toTest.length];
		double [][] delta_norm = new double[numIterations][reserProb_toTest.length];

		File fileOutput=new File(path + "Results.txt");
		BufferedWriter bw_bestparameters = new BufferedWriter(new FileWriter(fileOutput));

		DecimalFormat df = new DecimalFormat("#.############################");

		for(int factor = 0; factor<reserProb_toTest.length ; factor++){

			double[] last_pr = new double[(int)num_vertex] ;
			Arrays.fill(last_pr, 0);

			for(int iter = 0; iter < numIterations ; iter ++){

				int maxIter = (iter+1)*skip;
				long startTime = System.currentTimeMillis();
				PageRank pRank = gf.pageRank().resetProbability(reserProb_toTest[factor]).maxIter(maxIter);
				GraphFrame scores= pRank.run().unpersist().cache();
				long estimatedTime = System.currentTimeMillis() - startTime;
				times[iter][factor] = estimatedTime;
				double[] current_pr = asDoubleArray(scores.vertices().select("pagerank")
						.as(Encoders.DOUBLE()).collectAsList());
				delta_norm[iter][factor] = calculate_sqr_deltas(current_pr,last_pr);
				String line_wr = df.format(reserProb_toTest[factor]) +
						"\t" + iter +
						"\t" + maxIter +
						"\t" + estimatedTime +
						"\t" + df.format(delta_norm[iter][factor]) + "\n";
				bw_bestparameters.write(line_wr);
				System.out.println("[dampF: " + df.format(reserProb_toTest[factor]) +
						", maxIter: "+maxIter + "] -> Time: "+estimatedTime +
						" DeltaSQR: " + df.format(delta_norm[iter][factor]));

				last_pr = current_pr.clone();

			}
		}

		bw_bestparameters.close();

		double best_damping = 0.001;
		int best_maxIter = 10;

		PageRank pRank = gf.pageRank().resetProbability(best_damping).maxIter(best_maxIter);
		GraphFrame scores= pRank.run().unpersist().cache();
		scores.vertices().orderBy(org.apache.spark.sql.functions.col("pagerank").desc()).show();


		//scores.edges().show();

		//scores.inDegrees().show();

		//List<String> list_source = gf.vertices().select("id").as(Encoders.STRING()).collectAsList();

		//GraphFrame graph_parallel = gf.parallelPersonalizedPageRank().resetProbability(0.5).maxIter(10).sourceIds(list_source.toArray()).run();
		//graph_parallel.vertices().select("id", "pagerank").show();

	}
}
