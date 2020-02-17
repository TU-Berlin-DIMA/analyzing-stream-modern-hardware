package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.Generator;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;


public class YahooBenchmarkApp {
	public static final String usage = "usage: YahooBenchmarkApp with in-memory generation";

	public static void main (String [] args) throws InterruptedException {

		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 1;
		int batchSize = 4 * 1048576;
		String executionMode = "cpu";
		int circularBufferSize = 128 * 1 * 1048576/2;
		int unboundedBufferSize = 4 * 1048576;
		int hashTableSize = 2*64*128;
		int partialWindows = 2;
		int slots = 1 * 128 * 1024*2;

		boolean isV2 = false; // change the tuple size to half if set true
        String papi_preset = SystemConf.hwPerfCounters;
		/* Parse command line arguments */
		if (args.length!=0) {
			numberOfThreads = Integer.parseInt(args[0]);
			// papi_preset = args[1];
        }
        System.out.println("PRESET: " + papi_preset);
        SystemConf.hwPerfCounters = papi_preset;
		// Set SABER's configuration
		QueryConf queryConf = new QueryConf (batchSize);
		SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;
		SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;
		SystemConf.HASH_TABLE_SIZE = hashTableSize;
		SystemConf.PARTIAL_WINDOWS = partialWindows;
		SystemConf.SLOTS = slots;
		SystemConf.SWITCH_THRESHOLD = 10;
		SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;
		SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
		if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.CPU = true;
		if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
			SystemConf.GPU = true;
		SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
		SystemConf.THREADS = numberOfThreads;
		SystemConf.LATENCY_ON = false;


		/* Initialize the Operators of the Benchmark */
        // PAPIHardwareSampler[] operators_papi_samplers = new PAPIHardwareSampler[SystemConf.THREADS];
        // for (int i = 0; i < operators_papi_samplers.length; i ++) {
        //     operators_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        // }
        // PAPIHardwareSampler[] query_papi_samplers = new PAPIHardwareSampler[2];
        // for (int i = 0; i < query_papi_samplers.length; i ++) {
        //     query_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        // }
        PAPIHardwareSampler[] operators_papi_samplers = null;
        PAPIHardwareSampler[] query_papi_samplers = null;
		benchmarkQuery = new YahooBenchmark (queryConf, true, null, isV2, operators_papi_samplers,
                                             query_papi_samplers);

		/* Generate input stream */
		int numberOfGeneratorThreads = 2;
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();


		int bufferSize = 4 * 131072;
		int coreToBind = 3; //numberOfThreads + 1;
        PAPIHardwareSampler [] generator_papi_samplers = new PAPIHardwareSampler[numberOfGeneratorThreads];
        for (int i = 0; i < generator_papi_samplers.length; i ++) {
            generator_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        }

		Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind, isV2, generator_papi_samplers);
		long timeLimit = System.currentTimeMillis() + 100 * 1000;

		while (true) {

			if (timeLimit <= System.currentTimeMillis()) {
				System.out.println("Terminating execution...");

                try {

                    for (int i = 0; i < generator_papi_samplers.length; i ++) {
                        generator_papi_samplers[i].stopSampling("Generator Workers:");
                    }

                    for (int i = 0; i < operators_papi_samplers.length; i ++) {
                        operators_papi_samplers[i].stopSampling("Operator Workers:");
                    }
                    for (int i = 0; i < query_papi_samplers.length; i ++) {
                        query_papi_samplers[i].stopSampling("Circular Buffer Workers: ");
                    }
                } catch (Exception ex){

                }

				System.exit(0);
			}

			GeneratedBuffer b = generator.getNext();
            // if (generator.total_consumed_tuples >= 40 * 1000000) {
            //     try {

            //         for (int i = 0; i < generator_papi_samplers.length; i ++) {
            //             generator_papi_samplers[i].stopSampling("Generator Workers:");
            //         }

            //         for (int i = 0; i < operators_papi_samplers.length; i ++) {
            //             operators_papi_samplers[i].stopSampling("Operator Workers:");
            //         }
            //         for (int i = 0; i < query_papi_samplers.length; i ++) {
            //             query_papi_samplers[i].stopSampling("Circular Buffer Workers: ");
            //         }
            //     } catch (Exception ex){

            //     }
            //     System.out.println("Already consumed " + generator.total_consumed_tuples + " tuples");
            //     System.exit(0);
            // }
			benchmarkQuery.getApplication().processData (b.getBuffer().array());
			b.unlock();
		}
	}
}
