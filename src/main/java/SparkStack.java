import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Tuple2;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;
import org.apache.spark.api.java.function.Function;

public class SparkStack {
	
	
	private static JavaPairRDD<String,Double[]> last;

	private static Deque<JavaPairRDD<String,Double[]>> rddStack = new ArrayDeque<JavaPairRDD<String,Double[]>>();
	private static Deque<String> opStack = new ArrayDeque<String>();

	private static String idx_dir;
	private static String query;

	private static SnowballStemmer stemmer = new englishStemmer();
	
	private static JavaSparkContext sparkContext = new JavaSparkContext(SparkUI.sparkConf);

	public static List<String[]> perform(String input) throws JSONException, IOException, InterruptedException, ScriptException {
		
		idx_dir = SparkUI.idx_dir;
		query = input;
		String[] queryTerms = query.replaceAll("^\"|\"$", "").split("[ ]"); 
		
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF);
		
		iterateQuery(queryTerms);
		System.out.println("finished processing query");
		
		List<String[]> snippets = getSnippets(last); 

		return snippets;
	}
	
	private static void iterateQuery(String[] queryTerms) throws JSONException {
		System.out.println("Iterating queries...");

		for (String term : queryTerms) {
			switch (term) {
			case "(": break;
			case "and": opStack.push(term); break;
			case "or": opStack.push(term); break;
			case "andnot": opStack.push(term); break;
			case ")": mergeRDDs(); break;
			default: createRDD(term); break;
			}
		}
		System.out.println("Finished iterating queries...");
		last = rddStack.pop();
	}

	private static void createRDD(String term) throws JSONException {
		System.out.println("Creating RDD: " + term);

		stemmer.setCurrent(term);
		stemmer.stem();
		term = stemmer.getCurrent();
		final String finalTerm = term;
		String path = idx_dir + "/" + term.substring(0, 2);
		System.out.println("PATH: " + path);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile(path);
		JSONObject json = new SerializableJson(stringJavaRDD.filter(s -> 
		s.substring(StringUtils.ordinalIndexOf(s, "\"", 1) + 1, StringUtils.ordinalIndexOf(s, "\"", 2))
		.equals(finalTerm))
				.first());
		List<Tuple2<String,Double[]>> fileList = parse(term, json);
		System.out.println();
		rddStack.push(sparkContext.parallelizePairs(fileList));
		System.out.println("New rddStack Size: " + rddStack.size());
	}

	// parses out the file id list
	private static List<Tuple2<String,Double[]>> parse(String target, JSONObject json) throws JSONException {
		List<Tuple2<String,Double[]>> files = new ArrayList<Tuple2<String,Double[]>>();
		JSONObject temp = new JSONObject(json.toString());
		JSONArray arr = temp.getJSONArray(target);
		int n = arr.length();
		double ratio = 100.0/n;

		for (int i = 0; i < n; i++) {
			JSONObject singleDoc = arr.getJSONObject(i);
			@SuppressWarnings("unchecked")
			Iterator<String> iter = singleDoc.keys();
			String docID = iter.next();
			double score = (n-i)*ratio;
			String pos = ((String)singleDoc.get(docID)).trim();
			double firstPos = Double.parseDouble(pos);
			files.add(new Tuple2<String,Double[]>(docID, new Double[]{score, firstPos}));
		}
		return files;
	}

	private static void mergeRDDs() {
		System.out.println("Merging RDDs...");
		JavaPairRDD<String,Double[]> rdd2;
		JavaPairRDD<String,Double[]> rdd1;
		JavaPairRDD<String,Double[]> mergedRDD;

		if (!opStack.isEmpty()) {
			rdd2 = rddStack.pop();
			rdd1 = rddStack.pop();

			switch (opStack.pop()) {
			case "and": mergedRDD = operateAND(rdd1, rdd2); break;
			case "or": mergedRDD = operateOR(rdd1, rdd2); break;
			case "andnot": mergedRDD = operateSUB(rdd1, rdd2); break;
			default: mergedRDD = null; break;
			}
			rddStack.push(mergedRDD);
		}
	}

	private static JavaPairRDD<String,Double[]> operateAND(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
		List<String> commonKeys = set1.keys().intersection(set2.keys()).collect();
		JavaPairRDD<String,Double[]> commonRec = set1.filter(e1 -> commonKeys.contains(e1._1)).union(set2.filter(e2 -> commonKeys.contains(e2._1)));
		return commonRec.reduceByKey((Function2<Double[], Double[], Double[]>) (a, b) -> new Double[]{a[0]+b[0], Math.min(a[1], b[1])});
	}

	private static JavaPairRDD<String,Double[]> operateOR(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
		return set1.union(set2).reduceByKey((Function2<Double[], Double[], Double[]>) (a, b) -> new Double[]{a[0]+b[0], Math.min(a[1], b[1])});
	}

	private static JavaPairRDD<String,Double[]> operateSUB(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
		return set1.subtractByKey(set2);
	}	

	@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
	private static JavaRDD<Integer[]> rank(JavaPairRDD<String,Double[]> set) {
		return set.mapToPair(new PairFunction() {
			public Tuple2<Double,Integer[]> call(Object o) {
				Tuple2<String,Double[]> t = (Tuple2<String,Double[]>)o;
				return new Tuple2(t._2[0], new Integer[]{Integer.parseInt(t._1), t._2[1].intValue()});
			}
		}).sortByKey(false).values();
	}


	public static String getWikiFile(int wikiID) throws NumberFormatException, IOException {
		BufferedReader bf = new BufferedReader(new FileReader("/class/cs132/wiki_ranges.csv"));
		String line;
		while ((line = bf.readLine()) != null) {
			String[] entry = line.split(",");
			String wikiFile = entry[0];
			int start = Integer.parseInt(entry[1]);
			int stop = Integer.parseInt(entry[2]);
			if (wikiID >= start && wikiID <= stop) {
				bf.close();
				return wikiFile;
			}
		}
		bf.close();
		return "";
	}

	public static String getWikiArticle(int wikiID, String wikiFile) throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader("/class/cs132/wiki_csv/" + wikiFile));
		String article;
		while ((article = bf.readLine()) != null) {
			try {
				int currID = Integer.parseInt(article.substring(0, article.indexOf(",")));
				if (currID == wikiID) {
					bf.close();
					return article;
				}

			} catch (NumberFormatException e) {
				System.out.println(e);
			}
		}
		bf.close();
		return "";
	}
	
	
	private static List<String[]> getSnippets(JavaPairRDD<String,Double[]> rawResult) {
		JavaRDD<Integer[]> result = rank(rawResult);

		List<String[]> snips = result.map(new Function<Integer[], String[]>() {
			private static final long serialVersionUID = 1L;

			public String[] call(Integer[] s) throws Exception {
				int docId = s[0];
				int idx = s[1];
				String wikiFile = getWikiFile(docId);
				String article = getWikiArticle(docId, wikiFile);
				String[] row = article.split(",");
				int startIdx = Math.max(idx - 30, 0);
				int endIdx = Math.min(idx + 200, row[3].length());
				return new String[]{row[1],row[2],row[3].substring(startIdx, endIdx) + ". . ."};
			}
		}).take(25);

		return snips;
	}
}