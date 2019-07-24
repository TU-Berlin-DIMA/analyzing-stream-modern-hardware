package flink.lrb.kernel;

import flink.lrb.bean.InputRecord;
import flink.lrb.source.LinearRoadSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class LinearRoadBenchmark {

	private static final Logger LOG = LoggerFactory.getLogger(LinearRoadBenchmark.class);

	static public void main(String[] args) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final boolean preload = args[0].equals("1");
		final int numberOfFiles = Integer.parseInt(args[1]);
		String[] inputFiles = new String[numberOfFiles];
		int[] inputFilesLines = new int[numberOfFiles];

		for (int i = 0, j = 2; i < numberOfFiles; i++, j += 2) {
			inputFiles[i] = args[j];
			inputFilesLines[i] = Integer.parseInt(args[j + 1]);
		}

		DataStream<InputRecord> source = env
				.addSource(new LinearRoadSource(inputFiles, inputFilesLines, preload))
				.setParallelism(numberOfFiles);

		DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> intermediate =
			source.filter(new FilterFunction<InputRecord>() {
				@Override
				public boolean filter(InputRecord value) throws Exception {
					return value.getM_iType() == 0;
				}
			}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
				@Override
				public long extractAscendingTimestamp(InputRecord element) {
					return element.getM_iTime() * 1000;
				}
			}).map(new MapFunction<InputRecord, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
				@Override
				public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(InputRecord in) throws Exception {
					return new Tuple6<>(in.getM_iVid(), in.getM_iSeg(),
							in.getM_iSpeed(), in.getM_iPos(),
							in.getM_iTime(), in.getM_iXway());
				}
			})
		;


		// 0:vid, 1:seg, 2:speed, 3:pos, 4:time, 5:xway

		// speed monitor - segment, xway, avg speed
		DataStream<Tuple3<Integer, Integer, Double>> speedStreams = intermediate
			.keyBy(1, 5) // key by xway and seg
			.window(TumblingEventTimeWindows.of(Time.minutes(5)))
			.apply(new RichWindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
						Tuple3<Integer, Integer, Double>, Tuple, TimeWindow>() {


					@Override
					public void apply(Tuple tuple,
									  TimeWindow window,
									  Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
									  Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {

						Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();

						double speed = 0.0;
						long counter = 0L;

						while (it.hasNext()) {
							Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();
							speed += curr.f2;
							counter++;
						}

						Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> head = elements.iterator().next();

						out.collect(new Tuple3<>(head.f1, head.f5, speed / counter));
					}
				})
		;

		// vehicle stream - seg, xway, vcounter
		DataStream<Tuple3<Integer, Integer, Integer>> vehiclesStream = intermediate
			.keyBy( 1, 5)
			.window(TumblingEventTimeWindows.of(Time.minutes(1)))
			.apply(new WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
					Tuple3<Integer, Integer, Integer>, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple tuple,
								  TimeWindow window,
								  Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
								  Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

					Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();

					HashSet<Integer> counter = new HashSet<>();

					while (it.hasNext()) {
						Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();
						counter.add(curr.f0);
					}

					Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> head = elements.iterator().next();

					out.collect(new Tuple3<>(head.f1, head.f5, counter.size()));

				}
			})
		;

		// accident counter - vid, seg, xway, pos, time
		DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> accidentsStream = intermediate
			.keyBy(1) // segment
			.window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
			.process(new ProcessWindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
											Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow>() {

				transient private MapState<Integer, Tuple3<Integer, Integer, Long>> stopped;

				@Override
				public void process(Tuple tuple,
									Context context,
									Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
									Collector<Tuple5<Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
					Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();
					while (it.hasNext()) {

						Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();

						if (curr.f2 == 0) {

							int vid = curr.f0;
							int currPos = curr.f3;

							if (!stopped.contains(vid)) {
								stopped.put(vid, new Tuple3<>(1, currPos, context.currentProcessingTime()));
							} else {
								int oldPos = stopped.get(vid).f1;
								int cnt = stopped.get(vid).f0;
								long timestamp = stopped.get(vid).f2;
								long now = context.currentProcessingTime();
								if (currPos != oldPos || (now - timestamp) < (120 * 1000)) {
									// reset counter
									stopped.put(vid, new Tuple3<>(1, currPos, now));
								} else if (cnt == 4) {
									// possible accident
									out.collect(new Tuple5<>(vid, curr.f1, curr.f5, currPos, curr.f4));
								} else {
									// increase the counter
									stopped.put(vid, new Tuple3<>(cnt + 1, currPos, timestamp));
								}
							}
						}
					}
				}



				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);

					stopped = getRuntimeContext().getMapState(
							new MapStateDescriptor<>("stopped",
							TypeInformation.of(new TypeHint<Integer>() {}),
							TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>() {})));
				}

		});



		DataStream<Tuple4<Integer, Integer, Integer, Double>> tollsStream = vehiclesStream
			.join(speedStreams)
			.where(new KeySelector<Tuple3<Integer,Integer,Integer>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> getKey(Tuple3<Integer, Integer, Integer> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			})
			.equalTo(new KeySelector<Tuple3<Integer, Integer, Double>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> getKey(Tuple3<Integer, Integer, Double> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			})
			.window(TumblingEventTimeWindows.of(Time.seconds(30)))
			.apply(new JoinFunction<Tuple3<Integer,Integer,Integer>,
					Tuple3<Integer,Integer,Double>,
					Tuple4<Integer, Integer, Integer, Double>>() {
				@Override
				public Tuple4<Integer, Integer, Integer, Double> join(Tuple3<Integer, Integer, Integer> first,
																				Tuple3<Integer, Integer, Double> second) throws Exception {


					if (first.f2 > 50 && second.f2 < 40.0) {
						return new Tuple4<>(first.f0, first.f1, 2 * (first.f2 - 50) ^ 2, second.f2);
					} else {
						return new Tuple4<>(first.f0, first.f1, 0, second.f2);
					}

				}
			})
			.coGroup(accidentsStream)
			.where(new KeySelector<Tuple4<Integer,Integer,Integer,Double>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> getKey(Tuple4<Integer, Integer, Integer, Double> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			})
			.equalTo(new KeySelector<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> getKey(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
					return new Tuple2<>(value.f1, value.f2);
				}
			})
			.window(TumblingEventTimeWindows.of(Time.seconds(30)))
			.apply(new CoGroupFunction<Tuple4<Integer,Integer,Integer,Double>,
					Tuple5<Integer,Integer,Integer,Integer,Integer>,
					Tuple4<Integer, Integer, Integer, Double>>() {
				@Override
				public void coGroup(Iterable<Tuple4<Integer, Integer, Integer, Double>> first,
									Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> second,
									Collector<Tuple4<Integer, Integer, Integer, Double>> out) throws Exception {

					Iterator<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = second.iterator();
					Tuple4<Integer, Integer, Integer, Double> head = first.iterator().next();

					if (it.hasNext()) {
						out.collect(head);
					} else {
						out.collect(new Tuple4<>(head.f0, head.f1, 0, head.f3));
					}

				}
			})
		;


		tollsStream.print();


		try {
			String plan = env.getExecutionPlan();
			LOG.info("{}", plan);
			long start = System.currentTimeMillis();
			env.execute("Linear Road Benchmark");
			long end = System.currentTimeMillis();
			LOG.info("Running time: {} msec", end - start);
		} catch (Exception ex) {
			ex.printStackTrace();
		}



	}

}
