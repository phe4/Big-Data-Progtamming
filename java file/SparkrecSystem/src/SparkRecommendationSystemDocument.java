/*
 * this is used for the BigData programming class, for BDP-8-0*
 * when used for the BDP class, make sure you change the following line:
 * return value.split("\\W+");  use the following for English
 */
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkRecommendationSystemDocument {

	// change this to your own file path
	//C:/Users/89762/Desktop/CSC4760/HW6/sof_doc*.txt
	private static final String FILE_URI = "C:/Users/89762/Desktop/CSC4760/HW6/*.txt";
	public static void main(String[] args) {
		
		// initializing spark
		SparkSession spark = SparkSession.builder().config("spark.master","local[*]").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("WARN");
		
		// create RDD by reading text files
		System.out.println("Read text file:");
		JavaPairRDD<String,String> documents = sc.wholeTextFiles(FILE_URI);
		System.out.println(documents.take((int)documents.count()).toString());


		// break each document into words
		JavaPairRDD<Tuple2<String, String[]>, Long> wDocuments = documents.mapValues( new Function<String, String[]>() {
			public String[] call(String line) throws Exception {
				return line.split("\\W+");   // use the following for English
				// return line.split("\\|");  // use the following for Chinese
			}
		} ).zipWithIndex();
		System.out.println("Break each document into words:");
		System.out.println(wDocuments.take((int)wDocuments.count()).toString());
		
		
		// load wDocuments into dataframe
		StructType schema = new StructType(
				new StructField[] {
						DataTypes.createStructField("docID", DataTypes.LongType, false),
						DataTypes.createStructField("file_path", DataTypes.StringType, false),
						DataTypes.createStructField("all_words",DataTypes.createArrayType(DataTypes.StringType, false),false)
				});
		Dataset<Row> documentsWithAllWords = spark.createDataFrame(
				wDocuments.map( new Function<Tuple2<Tuple2<String,String[]>,Long>, Row>() {
					@Override
					public Row call(Tuple2<Tuple2<String,String[]>, Long> record) {
						return RowFactory.create(record._2(), record._1()._1().substring(record._1._1().lastIndexOf("/")+1), record._1()._2());
					}
				} ), schema);
		System.out.println("documents with all words:");
		documentsWithAllWords.show(true);
		
		
		// remove stop words
		StopWordsRemover remover = new StopWordsRemover().setInputCol("all_words").setOutputCol("words");
		Dataset<Row> documentsWithoutStopWords = remover.transform(documentsWithAllWords).select("docID", "file_path","words");
		System.out.println("documents without stop words:");
		documentsWithoutStopWords.show(true);
		
	    // fit a CountVectorizerModel from the corpus
		CountVectorizer vectorizer = new CountVectorizer().setInputCol("words").setOutputCol("TF_values");
		CountVectorizerModel cvm = vectorizer.fit(documentsWithoutStopWords);
		System.out.println("Vectorized documents:");
		System.out.println("vocab size = " + cvm.vocabulary().length);
		for (int i = 0; i < cvm.vocabulary().length; i ++ ) {
			System.out.print(cvm.vocabulary()[i] + "(" + i + ") ");
		}
		System.out.println();
	    Dataset<Row> tf = cvm.transform(documentsWithoutStopWords);
	    tf.show(true);

	    // Normalize each Vector using L1 norm.
	    Normalizer normalizer = new Normalizer().setInputCol("TF_values").setOutputCol("normalized_TF").setP(1.0);
	    Dataset<Row> normalizedTF = normalizer.transform(tf);
		System.out.println("Normalized documents:");
	    normalizedTF.show(true);
	    
	    // calcualte TF-IDF values
	    IDF idf = new IDF().setInputCol("normalized_TF").setOutputCol("TFIDF_values");
	    IDFModel idfModel = idf.fit(normalizedTF);
	    Dataset<Row> tf_idf = idfModel.transform(normalizedTF);
		System.out.println("tf_idf of documents:");
	    tf_idf.select("docID", "file_path", "words", "TFIDF_values").show(true);

	    
	    // ----------------FROM here below: you will need to put your new code -----------------
	    // I am suing columnSimilarities() below, but you will need to use the following:
	    // MinHashLSH mh = new MinHashLSH().setNumHashTables(100).setInputCol("binarized_feature").setOutputCol("minHashes");
	    // MinHashLSHModel model = mh.fit(binarizedDataFrame);  // you will need to create this binarizedDataFrame first
	    // --------------------------------------------------------------------------------------
	    
	    //the following is the new code
	    //Binarizer for the data frame of target and documents
	    Binarizer binarizer = new Binarizer()
	    		.setInputCol("TFIDF_values")
	    		.setOutputCol("binarized_feature")
	    		.setThreshold(0.01);
	    Dataset<Row> binarizedDataFrameD = binarizer.transform(tf_idf);
	    System.out.println("Binarized documents:");
	    binarizedDataFrameD.select("docID", "file_path", "words", "binarized_feature").show(true);

	    // take the target data out and save into a new dataframe
	    Dataset<Row>  binarizedDataFrameT= binarizedDataFrameD.filter("file_path == 'test_sof.txt'");
	    binarizedDataFrameT.show();
	    binarizedDataFrameD = binarizedDataFrameD.filter("file_path != 'test_sof.txt'");
	    
	    
	    //get vector key:
    	Vector key = (Vector) binarizedDataFrameT.first().getAs("binarized_feature");

    	// build minhash model
    	MinHashLSH mh = new MinHashLSH()
    			.setNumHashTables(100)
    			.setInputCol("binarized_feature")
    			.setOutputCol("minHashes");	
    	MinHashLSHModel model = mh.fit(binarizedDataFrameD);
    	System.out.println("The hashed dataset where hashed values are stored in the column 'hashes':");
    	model.transform(binarizedDataFrameD).select("docID", "file_path", "words", "minHashes").show();
    	System.out.println("recommended readings:");
    	model.approxNearestNeighbors(binarizedDataFrameD, key, 2).select("docID", "file_path", "words", "minHashes").show();  	

    	
    	
		spark.close();
	}

}

