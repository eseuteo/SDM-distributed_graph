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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class Exercise_4 {

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

		PageRank pRank = gf.pageRank().resetProbability(0.10).maxIter(20);
		GraphFrame scores= pRank.run().unpersist().cache();
		scores.vertices().select("id", "pagerank").show();

		//scores.vertices().show();
		//scores.edges().show();

		scores.vertices().groupBy().sum("pagerank").show();
		scores.vertices().groupBy().count().show();

		//scores.inDegrees().show();

		//List<String> list_source = gf.vertices().select("id").as(Encoders.STRING()).collectAsList();

		//GraphFrame graph_parallel = gf.parallelPersonalizedPageRank().resetProbability(0.5).maxIter(10).sourceIds(list_source.toArray()).run();
		//graph_parallel.vertices().select("id", "pagerank").show();

	}
}
