

import static spark.Spark.*;

import org.apache.spark.SparkConf;

public class SparkUI {

	public static String idx_dir;
	
	public static SparkConf sparkConf;
	
	public static void main(String[] args) {
		
		// set reference file dir
		idx_dir = args[0];
		
		// set spark configuration
		sparkConf = new SparkConf().
				setAppName("Spark Query").
				setMaster("local[*]").set("spark.ui.showConsoleProgress", "false"); // Delete this line when submitting to cluster
		
		// index request
		get("/", (req, res) -> IndexController.serveHomePage());
		
		// result request
		post("/result", (req, res) -> {
			String queryBody = req.body();
			String query = queryBody.split("=")[1];
			query = query.replaceAll("%28", "(");
			query = query.replaceAll("%29", ")");
			query = query.replace("+", " ");
			query = query.toLowerCase();
			
			return ResultController.serveResult(query);
		});
	}
	

}
