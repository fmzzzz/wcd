package hive;

import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RequestUDF extends UDF{
	Text output = new Text();
	StringBuffer input1 =null;
	public Text evaluate(Text str) {
		if(null == str) {
            return null;
        }
    	if(null == str.toString()) {
            return null;
        }
    	if (str.toString().indexOf(" /course/")==-1) {
			return null;
		}
    	if (str.toString().indexOf("?id=")==-1) {
    		return null;
    	}
    	
    	String input = str.toString();
    	StringTokenizer st = new StringTokenizer(input, " ", true);  
        StringBuffer sb = new StringBuffer();
        int i =1;
        while (st.hasMoreElements()) {
			input=st.nextToken();
			if (i==3) {
			input1 = sb.append(input);
				i++;
			}else {
				i++;
			}
		}
        
        if (input1.indexOf("&")!=-1) {
			String input2 = new String(input1);
			StringTokenizer st1 = new StringTokenizer(input2, "&", true); 
			  StringBuffer sb1 = new StringBuffer();
    	    int i1 =1;
            while (st1.hasMoreElements()) {
	   			input2=st1.nextToken();
	   			if (i1==1) {
	   				sb1.append(input2);
	   				i1++;
	   			}
           }
            output.set(sb1.toString());
    		return output;
		}
    	output.set(sb.toString());
		return output;
	}
	
	public static void main(String[] args) {
		System.out.println(new RequestUDF().evaluate(new Text("GET /course/view.php?id=10&p=3 HTTP/1.1")));
		System.out.println(new RequestUDF().evaluate(new Text("GET /course/view.php?id=10 HTTP/1.1")));
		System.out.println(new RequestUDF().evaluate(new Text("GET /course/image.php/bootstrap/core/1427679483/u/f1 HTTP/1.1")));
		System.out.println(new RequestUDF().evaluate(new Text("GET /theme/image.php/bootstrap/core/1427679483/u/f1 HTTP/1.1")));
	}
}
