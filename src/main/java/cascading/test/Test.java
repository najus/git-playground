package cascading.test;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class Test {

	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];

		TextLine scheme = new TextLine(new Fields("offset", "line"));

		Tap logTap = inputPath.matches("^[^:]+://.*") ? new Hfs(scheme,
				inputPath) : new Lfs(scheme, inputPath);

		Fields apacheFields = new Fields("ip", "time", "method", "event",
				"status", "size");

		String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";

		int[] allGroups = { 1, 2, 3, 4, 5, 6 };

		RegexParser parser = new RegexParser(apacheFields, apacheRegex,
				allGroups);

		Pipe importPipe = new Each("import", new Fields("line"), parser,
				Fields.RESULTS);

		Tap remoteLogTap = new Hfs(new TextLine(), outputPath, SinkMode.REPLACE);

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, Test.class);

		Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(
				logTap, remoteLogTap, importPipe);

		parsedLogFlow.start();
		parsedLogFlow.complete();
	}

}
