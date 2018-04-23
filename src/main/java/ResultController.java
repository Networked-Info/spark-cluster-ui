

import java.io.IOException;
import java.util.List;

import javax.script.ScriptException;

import org.json.JSONException;


public class ResultController {
	
	private String request;
	private String response;
	
	public ResultController(String request, String response){
		this.request = request;
		this.response = response;
	}
	
	public static String serveResult(String query) throws JSONException, IOException, InterruptedException, ScriptException {
		List<String[]> outputList = SparkStack.perform(query);
		return Page.resultString(outputList);
	}

}
