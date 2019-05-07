package de.dfki.iam.benchmarks.flink.ysb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

public class YSB {

	private static final Logger LOG = LoggerFactory.getLogger(YSB.class);

	private static final byte[] VIEW = new byte[] { 'v', 'i', 'e', 'w' };

	public static class YSBRecord implements Serializable {

		private final UUID f0;
		private final UUID f1;
		private final UUID f6;
		private final byte[] f2;
		private final  byte[] f3;
		private final long f4;
		private final int f5;

		public YSBRecord(UUID f0, UUID f1, UUID f6, byte[] f2,  byte[] f3, long f4, int f5) {
			this.f0 = f0;
			this.f1 = f1;
			this.f2 = f2;
			this.f3 = f3;
			this.f4 = f4;
			this.f5 = f5;
			this.f6 = f6;
		}

	}

	public static class YSBFinalRecord implements Serializable {

		private final UUID key;
		private final int value;

		public YSBFinalRecord(UUID k, int v) {
			this.key = k;
			this.value = v;
		}

	}


	public static class YSBRecordSerializer extends com.esotericsoftware.kryo.Serializer<YSBRecord> {

		@Override
		public void write(Kryo kryo, Output output, YSBRecord object) {
			output.writeLong(object.f0.getLeastSignificantBits());
			output.writeLong(object.f0.getMostSignificantBits());
			output.writeLong(object.f1.getLeastSignificantBits());
			output.writeLong(object.f1.getMostSignificantBits());
			output.writeLong(object.f6.getLeastSignificantBits());
			output.writeLong(object.f6.getMostSignificantBits());
			output.write(object.f2);
			output.write(object.f3);
			output.writeLong(object.f4);
			output.writeInt(object.f5);
		}

		private static byte[] readBytes(Input input, int count) {
			byte[] b = new byte[count];
			input.read(b);
			return b;
		}

		@Override
		public YSBRecord read(Kryo kryo, Input input, Class<YSBRecord> type) {
			return new YSBRecord(
					new UUID(input.readLong(), input.readLong()),
					new UUID(input.readLong(), input.readLong()),
					new UUID(input.readLong(), input.readLong()),
					readBytes(input,9),
					readBytes(input,9),
					input.readLong(),
					input.readInt());
		}
	}

	public static class YSBFinalRecordSerializer extends com.esotericsoftware.kryo.Serializer<YSBFinalRecord> {

		@Override
		public void write(Kryo kryo, Output output, YSBFinalRecord object) {
			output.writeLong(object.key.getLeastSignificantBits());
			output.writeLong(object.key.getMostSignificantBits());
			output.writeInt(object.value);
		}

		private static byte[] readBytes(Input input, int count) {
			byte[] b = new byte[count];
			input.read(b);
			return b;
		}

		@Override
		public YSBFinalRecord read(Kryo kryo, Input input, Class<YSBFinalRecord> type) {
			return new YSBFinalRecord(
					new UUID(input.readLong(), input.readLong()),
					input.readInt());
		}
	}


	public static class YSBSource extends RichParallelSourceFunction<YSBRecord> {

		private static final Logger LOG = LoggerFactory.getLogger(YSBSource.class);

		private volatile boolean running = true;

		private final int numOfRecords;

		private transient MappedByteBuffer mbuff;

		private transient FileChannel channel;

		private final String path;

		public YSBSource(String path, int numOfRecords) {
			this.path = path;
			this.numOfRecords = numOfRecords;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int idx = getRuntimeContext().getIndexOfThisSubtask();

			channel = FileChannel.open(new File(path + "/ysb" + idx + ".bin").toPath(), StandardOpenOption.READ);
			mbuff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
		}


		@Override
		public void close() throws Exception {
			channel.close();
		}

		@Override
		public void run(SourceContext<YSBRecord> ctx) throws Exception {
			long start = System.nanoTime();
			for (int i = 0; i < numOfRecords; i++) {
				UUID u0 = new UUID(mbuff.getLong(), mbuff.getLong());
				UUID u1 = new UUID(mbuff.getLong(), mbuff.getLong());
				UUID u2 = new UUID(mbuff.getLong(), mbuff.getLong());
				byte[] b0 = new byte[9];
				byte[] b1 = new byte[9];
				mbuff.get(b0);
				mbuff.get(b1);
				long l0 = mbuff.getLong();
				int i0 = mbuff.getInt();
				ctx.collect(new YSBRecord(u0, u1, u2, b0, b1, l0, i0)); // filtering is possible also here but it d not be idiomatic
			}
			double diff = System.nanoTime() - start;
			double diffMs = diff / 1_000_000;
			double throughput = numOfRecords / (diffMs / 1000);
			LOG.info("YSB took {} ms -> {} records/sec", diffMs, throughput);
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class YSBSourceOptimized extends RichParallelSourceFunction<YSBFinalRecord> {

		private static final Logger LOG = LoggerFactory.getLogger(YSBSourceOptimized.class);

		private volatile boolean running = true;

		private final int numOfRecords;

		private transient MappedByteBuffer mbuff;

		private transient FileChannel channel;

		private final String path;

		public YSBSourceOptimized(String path, int numOfRecords) {
			this.path = path;
			this.numOfRecords = numOfRecords;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			int idx = getRuntimeContext().getIndexOfThisSubtask();

			channel = FileChannel.open(new File(path + "/ysb" + idx + ".bin").toPath(), StandardOpenOption.READ);
			mbuff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
		}


		@Override
		public void close() throws Exception {
			channel.close();
		}

		@Override
		public void run(SourceContext<YSBFinalRecord> ctx) throws Exception {
			long start = System.nanoTime();
			for (int i = 0; i < numOfRecords; i++) {
				mbuff.getLong();
				mbuff.getLong();
				mbuff.getLong();
				mbuff.getLong();
				UUID u2 = new UUID(mbuff.getLong(), mbuff.getLong());
				byte[] b0 = new byte[9];
				byte[] b1 = new byte[9];
				mbuff.get(b0);
				mbuff.get(b1);
				long l0 = mbuff.getLong();
				int i0 = mbuff.getInt();
				if (b1[0] == VIEW[0]) {
					ctx.collect(new YSBFinalRecord(u2, i0));
				}
//				ctx.collect(new YSBRecord(u0, u1, u2, b0, b1, l0, i0)); // filtering is possible also here but it d not be idiomatic
			}
			double diff = System.nanoTime() - start;
			double diffMs = diff / 1_000_000;
			double throughput = numOfRecords / (diffMs / 1000);
			LOG.info("YSB took {} ms -> {} records/sec", diffMs, throughput);
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static void main(String[] args) throws Exception {

		boolean optimized = false;

		ParameterTool params = ParameterTool.fromArgs(args);
		final int sourceParallelism = params.getInt("sourceParallelism", 1);
		final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 0);
		final int parallelism = params.getInt("parallelism", 1);
		final int maxParallelism = params.getInt("maxParallelism", 10_000);
		final int numOfRecords = params.getInt("numOfRecords");
		final String dataPath = params.get("path");

		LOG.info("Arguments: {}", params);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env.setParallelism(parallelism);
		env.getConfig().enableObjectReuse();
		env.setMaxParallelism(maxParallelism);
		env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		env.getConfig().enableForceKryo();
		env.getConfig().registerTypeWithKryoSerializer(YSBRecord.class, YSBRecordSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(YSBFinalRecord.class, YSBFinalRecordSerializer.class);
		env.getConfig().addDefaultKryoSerializer(YSBRecord.class, YSBRecordSerializer.class);
		env.getConfig().addDefaultKryoSerializer(YSBFinalRecord.class, YSBFinalRecordSerializer.class);
		env.getConfig().registerKryoType(YSBRecord.class);
		env.getConfig().registerKryoType(YSBFinalRecord.class);

		if (optimized) {
			env
					.addSource(new YSBSourceOptimized(dataPath, numOfRecords))
					.setParallelism(sourceParallelism)
					.keyBy((KeySelector<YSBFinalRecord, UUID>) r -> r.key)
					.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
					.aggregate(new WindowingLogic())
					.setMaxParallelism(maxParallelism)
					.name("WindowOperator")
					.addSink(new SinkFunction<Long>() {
						@Override
						public void invoke(Long value) throws Exception {

						}
					});

		} else {
			env
					.addSource(new YSBSource(dataPath, numOfRecords))
					.setParallelism(sourceParallelism)
					.flatMap(new Filter())
					.setParallelism(sourceParallelism)
					.keyBy((KeySelector<YSBFinalRecord, UUID>) r -> r.key)
					.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
					.aggregate(new WindowingLogic())
					.setMaxParallelism(maxParallelism)
					.name("WindowOperator")
					.addSink(new SinkFunction<Long>() {
						@Override
						public void invoke(Long value) throws Exception {

						}
					});
		}
		env.execute("yahoo streaming benchmark");

	}


	private static class WindowingLogic implements AggregateFunction<YSBFinalRecord, Long, Long> {
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(YSBFinalRecord value, Long acc) {
			return acc + value.value;
		}

		@Override
		public Long getResult(Long acc) {
			return acc;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	private static class Filter implements FlatMapFunction<YSBRecord, YSBFinalRecord> {

		@Override
		public void flatMap(YSBRecord in, Collector<YSBFinalRecord> out) throws Exception {
			if (in.f3[0] == VIEW[0] && in.f3[1] == VIEW[1] && in.f3[2] == VIEW[2] && in.f3[3] == VIEW[3]) { // wish for simd
				out.collect(new YSBFinalRecord(in.f6, in.f5));
			}
		}
	}

}
