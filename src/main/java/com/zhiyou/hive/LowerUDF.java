package com.zhiyou.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class LowerUDF extends UDF{
	
	public Text evaluate(Text str) {
		if (null==str.toString()){
			return null;
		}
		return new Text(str.toString().toLowerCase());
	}
	public static void main(String[] args) {
		System.out.println(new LowerUDF().evaluate(new Text("Hive")));
	}
}
