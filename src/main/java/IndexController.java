
public class IndexController {
	
	private String request;
	private String response;
	
	public IndexController(String request, String response) {
		this.request = request;
		this.response = response;
	}
	
	public static String serveHomePage() {
		return Page.indexString();
	}

}
