package hive;
import java.util.StringTokenizer;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
public class IpSplitUDF extends UDF{

	Text output = new Text();
    public Text evaluate(Text str) {
    	if(null == str) {
            return null;
        }
    	if(null == str.toString()) {
            return null;
        }
        String input = str.toString();
        //evaluate
        StringTokenizer st = new StringTokenizer(input, ".", true);  
        StringBuffer sb = new StringBuffer();
        int i = 1;
        while(st.hasMoreElements()){  
            input = st.nextToken();
             if(i <= 3) {
                 sb.append(input);
                 i++;
             }
        }  
        output.set(sb.toString());
        return output;
    }
    public static void main(String[] args) {
    	System.out.println(new IpSplitUDF().evaluate(new Text("192.168.1.112")));
    }
}
