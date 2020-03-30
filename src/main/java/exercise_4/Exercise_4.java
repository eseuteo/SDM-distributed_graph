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
import java.util.List;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws Exception{

		String  path = "/home/ricardohb/Documents/SDM/lab2/SparkGraphXassignment/src/main/resources/";

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

		int[] maxIter_toTest = {1,5,10,25,50,100};
		double[] dampingFactor_toTest = {0.0001,0.001,0.1,0.15,0.2,0.3,0.5};
		maxIter_toTest = new int[]{};
		dampingFactor_toTest = new double[]{};

		//int[] maxIter_toTest = {1};
		//double[] dampingFactor_toTest = {0.0001,0.001};

		long [][] times = new long[maxIter_toTest.length][dampingFactor_toTest.length];
		double [][] sum_error = new double[maxIter_toTest.length][dampingFactor_toTest.length];

		File fileOutput=new File(path + "Results.txt");
		BufferedWriter bw_bestparameters = new BufferedWriter(new FileWriter(fileOutput));

		DecimalFormat df = new DecimalFormat("#.############################");

		for(int iter = 0; iter < maxIter_toTest.length ; iter ++){
			for(int factor = 0; factor<dampingFactor_toTest.length ; factor++){

				long startTime = System.currentTimeMillis();
				PageRank pRank = gf.pageRank().resetProbability(dampingFactor_toTest[factor]).maxIter(maxIter_toTest[iter]);
				GraphFrame scores= pRank.run().unpersist().cache();
				long estimatedTime = System.currentTimeMillis() - startTime;
				times[iter][factor] = estimatedTime;
				sum_error[iter][factor] = scores.vertices().groupBy().sum("pagerank")
						.as(Encoders.DOUBLE()).collectAsList().get(0) - num_vertex;
				String line_wr = df.format(dampingFactor_toTest[factor]) +
						"\t" + maxIter_toTest[iter] +
						"\t" + maxIter_toTest[iter] +
						"\t" + estimatedTime +
						"\t" + df.format(sum_error[iter][factor]) + "\n";
				bw_bestparameters.write(line_wr);
				System.out.println("[dampF: " + df.format(dampingFactor_toTest[factor]) +
						", maxIter: "+maxIter_toTest[iter] + "] -> Time: "+estimatedTime +
						" SumError: " + df.format(sum_error[iter][factor]));
			}
		}

		bw_bestparameters.close();

		double best_damping = 0.001;
		int best_maxIter = 10;

		PageRank pRank = gf.pageRank().resetProbability(best_damping).maxIter(best_maxIter);
		GraphFrame scores= pRank.run().unpersist().cache();
		scores.vertices().select("id", "pagerank").show();

		scores.vertices().orderBy(org.apache.spark.sql.functions.col("pagerank").desc()).show();
		//scores.edges().show();

		//scores.inDegrees().show();

		//List<String> list_source = gf.vertices().select("id").as(Encoders.STRING()).collectAsList();

		//GraphFrame graph_parallel = gf.parallelPersonalizedPageRank().resetProbability(0.5).maxIter(10).sourceIds(list_source.toArray()).run();
		//graph_parallel.vertices().select("id", "pagerank").show();

	}
}
