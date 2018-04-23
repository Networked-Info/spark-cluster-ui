

import java.util.List;

public class Page {
	
	public static String indexString() {
		return "<!DOCTYPE html>" + 
				"<html>" + 
				"<body background=\"https://www.desktopbackground.org/p/2014/02/17/718653_subtle-backgrounds-on-pinterest_1800x1200_h.jpg\">" +
				"<h1>XCN KittenSearch</h1>" +
				"<img src=\"https://i.imgflip.com/14tdq0.jpg\">" +
				"<br>" +
				"<h4>Please input your query</h4>" + 
				"<form method=\"POST\" action=\"/result\">" +
				"<input type=\"text\" name=\"query\">" +
				"<br>" +
				"<input type=\"submit\" value=\"Submit\">" +
				"</form>" +
				"</body>" +
				"</html>";
	}
	
	public static String resultString(List<String[]> resultList) {
		String output = "<!DOCTYPE html>" +
				"<body background=\"https://www.desktopbackground.org/p/2014/02/17/718653_subtle-backgrounds-on-pinterest_1800x1200_h.jpg\">"+
				"<h1>XCN KittenSearch</h1>" +
				"<img src=\"https://i.imgflip.com/14tdq0.jpg\">" +
				"<br>" +
				"<h4> Results </h4>" +
				"<form method=\"GET\" action=\"/\">" +
				"<input type=\"submit\" value=\"Search More\">" + 
				"</form>" + 
				"<div>" + 
				"<table>";
		
		for (String[] result : resultList) {
			output = output + "<tr bgcolor=\"white\">" + 
					"<h2>" + 
						"<a target=\"_blank\" href=\"" + result[0] + "\">" + 
							result[1] +
						"</a>" +
					"</h2>" +
					" -- " + result[2] + "</tr><br><hr>";
		}
		
		output = output + "</table>" + "</div>" + "</body>";
		
		return output;
	}

}
