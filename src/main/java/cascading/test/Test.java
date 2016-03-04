package cascading.test;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;

public class Test {

	private static void process(String input, String output) {

		Scheme sourceScheme = new TextLine(Fields.ALL);
		Scheme sinkScheme = new TextLine(new Fields("word", "count"));

	}

	public static void main(String[] args) {

	}

}
