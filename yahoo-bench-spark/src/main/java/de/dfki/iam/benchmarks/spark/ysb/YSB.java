package de.dfki.iam.benchmarks.spark.ysb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Executors;

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


	public static void main(String[] args) throws Exception {

		int numOfInputFiles = Integer.parseInt(args[0]);
		int numOfRecords = Integer.parseInt(args[1]);
		String dataPath = args[2];
		String masterUrl = args[3];
		SparkConf sparkConf = new SparkConf()
				.setMaster(masterUrl)
				.setAppName("Yahoo Streaming Bench");

		sparkConf.registerKryoClasses(new Class[] { YSBRecord.class });

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		ArrayList<JavaDStream<YSBRecord>> inputs = new ArrayList<>();

		for (int k = 0; k < numOfInputFiles; k++) {
			final int idx = k;
			inputs.add(ssc.receiverStream(new Receiver<YSBRecord>(StorageLevel.MEMORY_ONLY_SER()) {

				private boolean isStarted = false;

				@Override
				public void onStart() {
					if (isStarted) {
						return;
					}
					Runnable task = new Runnable() {
						@Override
						public void run() {
							try {
								FileChannel channel = FileChannel.open(new File(String.format("%s/ysb%d.bin", dataPath, idx)).toPath(), StandardOpenOption.READ);
								MappedByteBuffer mbuff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
								long start = System.nanoTime();
								store(new Iterator<YSBRecord>() {
									private final FileChannel fileChannel = channel; // to prevent GC
									private final MappedByteBuffer mappedBuffer = mbuff;
									private int currIndex = 0;

									@Override
									public boolean hasNext() {
										return currIndex < numOfRecords;
									}

									@Override
									public YSBRecord next() {
										try {
											UUID u0 = new UUID(mappedBuffer.getLong(), mappedBuffer.getLong());
											UUID u1 = new UUID(mappedBuffer.getLong(), mappedBuffer.getLong());
											UUID u2 = new UUID(mappedBuffer.getLong(), mappedBuffer.getLong());
											byte[] b0 = new byte[9];
											byte[] b1 = new byte[9];
											mappedBuffer.get(b0);
											mappedBuffer.get(b1);
											long l0 = mappedBuffer.getLong();
											int i0 = mappedBuffer.getInt();
											currIndex++;
											return new YSBRecord(u0, u1, u2, b0, b1, l0, i0);
										} catch (Throwable t) {
											throw t;
										}
									}
								});
//								stop("completed");
								long diff = System.nanoTime() - start;
								long diffMs = diff / 1_000_000;
								long throughput = numOfRecords / (diffMs / 1000);
								LOG.info("YSB took {} ms -> {} records/sec", diffMs, throughput);
							} catch (Exception e) {
								LOG.error("Error {}", e);
							}

						}
					};
					Thread t = new Thread(task);
					t.setDaemon(true);
					t.setName("Ysb source " + idx);
					t.start();
					LOG.info("Launched Ysb source " + idx);
					isStarted = true;
				}

				@Override
				public void onStop() {
				}
			}));
		}

		JavaDStream<YSBRecord> in = inputs.size() == 1 ? inputs.get(0) : ssc.union(inputs.get(0), inputs.subList(1, inputs.size() - 1));

		JavaPairDStream<UUID, Integer> out = in
				.flatMapToPair(new PairFlatMapFunction<YSBRecord, UUID, Integer>() {
					@Override
					public Iterator<Tuple2<UUID, Integer>> call(YSBRecord rec) throws Exception {
						if (rec.f3[0] == VIEW[0] && rec.f3[1] == VIEW[1] && rec.f3[2] == VIEW[2] && rec.f3[3] == VIEW[3]) {
							return Collections.singleton(new Tuple2<>(rec.f6, rec.f5)).iterator();
						}
						return Collections.emptyIterator();
					}
				})
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) throws Exception {
						return a + b;
					}
				}, Durations.seconds(2));

		out.foreachRDD((VoidFunction2<JavaPairRDD<UUID, Integer>, Time>) (rrd, time) ->
				rrd.foreach((VoidFunction<Tuple2<UUID, Integer>>) a -> {

				}
			)
		);

		ssc.start();
		ssc.awaitTermination();
	}



}
