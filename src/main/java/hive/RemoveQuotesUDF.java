package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RemoveQuotesUDF extends UDF{
	//evaluate
    public Text evaluate(Text str) {
        if(null == str) {
            return null;
        }
        if(null == str.toString()) {
            return null;
        }
        /*如果str是null，.toString()会变成空指针
         * if(null == str.toString()) {
            return new Text();
        }*/
        return new Text(str.toString().replace("\"", ""));
    }
    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"hive\"")));
    }
}

