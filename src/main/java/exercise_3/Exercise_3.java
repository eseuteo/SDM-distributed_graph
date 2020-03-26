package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;

public class Exercise_3 {
    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer, ArrayList<Long>>,Tuple2<Integer, ArrayList<Long>>,Tuple2<Integer, ArrayList<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<Long>> apply(Long vertexID, Tuple2<Integer, ArrayList<Long>> vertexValue, Tuple2<Integer, ArrayList<Long>> message) {
            ArrayList<Long> pathSelected = vertexValue._1 < message._1 ? vertexValue._2 : message._2;
            if (message._2 == null) {
                pathSelected = vertexValue._2;
            }
            Tuple2<Integer, ArrayList<Long>> answer = new Tuple2<Integer, ArrayList<Long>>(Math.min(vertexValue._1, message._1), pathSelected);
            return answer;
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, ArrayList<Long>>,Tuple2<Integer, ArrayList<Long>>>, Iterator<Tuple2<Object,Tuple2<Integer, ArrayList<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, ArrayList<Long>>>> apply(EdgeTriplet<Tuple2<Integer, ArrayList<Long>>, Tuple2<Integer, ArrayList<Long>>> triplet) {
            Tuple2<Object,Tuple2<Integer, ArrayList<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Integer, ArrayList<Long>>> dstVertex = triplet.toTuple()._2();
            Integer edgeWeight = triplet.toTuple()._3()._1;
            if (!sourceVertex._2._1.equals(Integer.MAX_VALUE) && dstVertex._2._1 > sourceVertex._2._1 + edgeWeight) {
                sourceVertex._2._2.add(triplet.dstId());
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Integer, ArrayList<Long>>>(triplet.dstId(), new Tuple2<Integer, ArrayList<Long>>(sourceVertex._2._1 + edgeWeight, sourceVertex._2._2))).iterator()).asScala();
            } else {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, ArrayList<Long>>>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, ArrayList<Long>>,Tuple2<Integer, ArrayList<Long>>,Tuple2<Integer, ArrayList<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<Long>> apply(Tuple2<Integer, ArrayList<Long>> o, Tuple2<Integer, ArrayList<Long>> o2) {
            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<>(1l, new Tuple2<>(0, new ArrayList<Long>(){{add(1l);}})),
                new Tuple2<>(2l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<Long>(){{add(2l);}})),
                new Tuple2<>(3l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<Long>(){{add(3l);}})),
                new Tuple2<>(4l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<Long>(){{add(4l);}})),
                new Tuple2<>(5l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<Long>(){{add(5l);}})),
                new Tuple2<>(6l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<Long>(){{add(6l);}}))
        );

        List<Edge<Tuple2<Integer,ArrayList<Long>>>> edges = Lists.newArrayList(
                new Edge<Tuple2<Integer, ArrayList<Long>>>(1l,2l, new Tuple2<Integer, ArrayList<Long>>(4, new ArrayList<Long>())), // A --> B (4)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(1l,3l, new Tuple2<Integer, ArrayList<Long>>(2, new ArrayList<Long>())), // A --> C (2)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(2l,3l, new Tuple2<Integer, ArrayList<Long>>(5, new ArrayList<Long>())), // B --> C (5)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(2l,4l, new Tuple2<Integer, ArrayList<Long>>(10, new ArrayList<Long>())), // B --> D (10)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(3l,5l, new Tuple2<Integer, ArrayList<Long>>(3, new ArrayList<Long>())), // C --> E (3)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(5l, 4l, new Tuple2<Integer, ArrayList<Long>>(4, new ArrayList<Long>())), // E --> D (4)
                new Edge<Tuple2<Integer, ArrayList<Long>>>(4l, 6l, new Tuple2<Integer, ArrayList<Long>>(11, new ArrayList<Long>())) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Tuple2<Integer,ArrayList<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Tuple2<Integer, ArrayList<Long>>>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer, ArrayList<Long>>, Tuple2<Integer, ArrayList<Long>>> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<Integer,ArrayList<Long>>(1, new ArrayList<Long>()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(Tuple2.class), ClassTag$.MODULE$.apply(Tuple2.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));

        ops.pregel(new Tuple2<Integer, ArrayList<Long>>(Integer.MAX_VALUE,null),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.VProg(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(f -> ((Tuple2<Object, Tuple2<Integer, ArrayList<Long>>>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Tuple2<Integer, ArrayList<Long>>> vertex = (Tuple2<Object,Tuple2<Integer, ArrayList<Long>>>)v;
                    System.out.println("Shortest path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is " + getVerticesLabels(vertex._2._2, labels) + " with cost " + vertex._2._1);
                });
    }

    private static String getVerticesLabels(ArrayList<Long> indexes, Map<Long, String> labels) {
        StringBuilder sb = new StringBuilder("[");
        String prefix = "";
        for (Long item : indexes) {
            sb.append(prefix);
            prefix = ", ";
            sb.append(labels.get(item));
        }
        return sb.append("]").toString();
    }
}
