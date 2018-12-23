package hive;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

//@Description()
public class DateTransformUDF extends UDF{

    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /*31/Aug/2017:00:04:37 +0800
    2017-08-31 00:04:37*/
    
    public Text evaluate(Text input) {
        
        Text output = new Text();
        
        //evaluate
        if(null == input) {
            return null;
        }
        /*trim()方法是用来删除字符串两端的空白字符并返回，
        trim方法并不影响原来的字符串本身，它返回的是一个新的字符串*/
        
  String inputDate = input.toString().trim();
        if(null == inputDate) {
            return null;
        }
        try {
            //转换
            Date parseDate = inputFormat.parse(inputDate);
            //tranform
            String outputDate = outputFormat.format(parseDate);
            //set
            output.set(outputDate);
        }catch(Exception e) {
            e.printStackTrace();
            return output;
        }
        return output;
    }
    public static void main(String[] args) {
        new DateTransformUDF().evaluate(new Text("31/Aug/2017:00:04:37 +0800"));
    }
}
