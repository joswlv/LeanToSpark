package udf;

import org.apache.spark.sql.api.java.UDF3;

import java.io.Serializable;

public class DspLogParser implements UDF3<String, String, String, String> {

	private static final long serialVersionUID = -20170805093001L;

	@Override
	public String call(String str, String delimiter, String key) throws Exception {
		if (null == str) {
			return null;
		}

		String result = null;

		int idx = 0;

		if (str.startsWith("\"")) {
			str = str.substring(1);
		}
		if (str.endsWith("\"")) {
			str = str.substring(0, str.length() - 1);
		}

		if ("&".equals(delimiter) && (idx = str.indexOf('?')) > 0) {
			str = str.substring(idx + 1);
		}

		String arr[] = str.split(delimiter);
		for (String s1 : arr) {
			if ((idx = s1.indexOf('=')) > 0) {
				if (key.equals(s1.substring(0, idx))) {
					result = s1.substring(idx + 1);
					break;
				}
			}
		}
		return result;
	}
}
