package cascading.test;

import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class LogAnalysis {
	public static void main(String[] args) {
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, LogAnalysis.class);

		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		CascadeConnector cascadeConnector = new CascadeConnector(properties);

		String inputPath = args[0];
		String logsPath = args[1] + "/logs/";
		String arrivalRatePath = args[1] + "/arrivalrate/";
		String arrivalRateSecPath = arrivalRatePath + "sec";
		String arrivalRateMinPath = arrivalRatePath + "min";

		Fields apacheFields = new Fields("ip", "time", "method", "event",
				"status", "size");
		String apacheRegex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
		int[] apacheGroups = { 1, 2, 3, 4, 5, 6 };
		RegexParser parser = new RegexParser(apacheFields, apacheRegex,
				apacheGroups);
		Pipe importPipe = new Each("import", new Fields("line"), parser);

		Tap logTap = inputPath.matches("^[^:]+://.*") ? new Hfs(new TextLine(),
				inputPath) : new Lfs(new TextLine(), inputPath);
		Tap parsedLogTap = new Hfs(apacheFields, logsPath);

		Flow importLogFlow = flowConnector.connect(logTap, parsedLogTap,
				importPipe);

		DateParser dateParser = new DateParser(new Fields("ts"),
				"dd/MMM/yyyy:HH:mm:ss Z");
		Pipe tsPipe = new Each("arrival rate", new Fields("time"), dateParser,
				Fields.RESULTS);
		Pipe tsCountPipe = new Pipe("tsCount", tsPipe);
		tsCountPipe = new GroupBy(tsCountPipe, new Fields("ts"));
		tsCountPipe = new Every(tsCountPipe, Fields.GROUP, new Count());

		Pipe tmPipe = new Each(tsPipe, new ExpressionFunction(new Fields("tm"),
				"ts - (ts % (60 * 1000))", long.class));

		Pipe tmCountPipe = new Pipe("tmCount", tmPipe);
		tmCountPipe = new GroupBy(tmCountPipe, new Fields("tm"));
		tmCountPipe = new Every(tmCountPipe, Fields.GROUP, new Count());

		Tap tsSinkTap = new Hfs(new TextLine(), arrivalRateSecPath);
		Tap tmSinkTap = new Hfs(new TextLine(), arrivalRateMinPath);

		FlowDef flowDef = FlowDef.flowDef();

		flowDef.addSource(tsPipe, parsedLogTap);
		flowDef.addTailSink(tsCountPipe, tsSinkTap);
		flowDef.addTailSink(tmCountPipe, tmSinkTap);

		Flow arrivalRateFlow = flowConnector.connect(flowDef);

		Cascade cascade = cascadeConnector.connect(importLogFlow,
				arrivalRateFlow);

		cascade.complete();
	}

}
