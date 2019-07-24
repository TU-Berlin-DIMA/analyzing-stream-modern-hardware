package flink.lrb.source;

import flink.lrb.bean.InputRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class LinearRoadSource extends RichParallelSourceFunction<InputRecord> {

	private static final Logger LOG = LoggerFactory.getLogger(LinearRoadSource.class);

	private boolean running = true;
	private boolean preload = true;

	private byte[] buffer = new byte[8192];
	private FileInputStream fis;
	private BufferedReader reader;

	private String[] files;
	private int[] lines;

	private InputRecord[] cached;

	public LinearRoadSource(String[] files, int[] lines, boolean preload) {
		this.files = files;
		this.lines = lines;
		this.preload = preload;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		final int parallelInstanceIndex = getRuntimeContext().getIndexOfThisSubtask();
		final int end = lines[parallelInstanceIndex];
		fis = new FileInputStream(files[parallelInstanceIndex]);
		reader = new BufferedReader(new InputStreamReader(fis));

		if (preload) {
			cached = new InputRecord[lines[parallelInstanceIndex]];
			for (int i = 0; i < end; i++) {
				String line = reader.readLine();
				cached[i] = new InputRecord(line);
			}

		}

	}

	@Override
	public void close() throws Exception {
		super.close();
		fis.close();
		reader.close();
	}

	@Override
	public void run(SourceContext<InputRecord> ctx) throws Exception {
		long start_time = System.currentTimeMillis();
		final int idx = getRuntimeContext().getIndexOfThisSubtask();
		final int end = lines[idx];
		final Counter throughputCounter = getRuntimeContext().getMetricGroup().counter("throughput-counter");
		final Meter throughputMeter = getRuntimeContext().getMetricGroup().meter("throughput-meter", new MeterView(throughputCounter, 60));
		if (preload) {
			for (int i = 0; i < end; i++) {
				ctx.collect(cached[i]);
				throughputCounter.inc();
			}
		} else {
			for (int i = 0; i < end; i++) {
				String line = reader.readLine();
				ctx.collect(new InputRecord(line));
				throughputCounter.inc();
			}
		}
		ctx.close();
		long end_time = System.currentTimeMillis();
		LOG.info("GREPSTATISTICS :: Source Operator {} closing - counter=[{} records] thr=[{} records/sec] diff=[{} msec]", idx, throughputCounter.getCount(), throughputMeter.getRate(), end_time - start_time);
	}

	@Override
	public void cancel() {
		running = false;
	}
}
