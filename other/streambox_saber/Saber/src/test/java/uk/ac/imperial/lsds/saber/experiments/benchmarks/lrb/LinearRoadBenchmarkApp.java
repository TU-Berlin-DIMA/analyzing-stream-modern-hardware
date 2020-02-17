package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;

import uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils.Generator;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;

public class LinearRoadBenchmarkApp {

    public static void main (String [] args) throws InterruptedException, IOException {
        System.out.println("Usage: java class numberOfThreads filename lineToReads PRESET");
        int totalLines = 1000000;
        int numberOfThreads = 1;
        // int totalLines = Integer.MAX_VALUE;
        String path = "/home/zongxiong/cardatapoints.out0";
        String papi_preset = SystemConf.hwPerfCounters;
        /* Parse command line arguments */
        if (args.length!=0) {
            numberOfThreads = Integer.parseInt(args[0]);
            // path = args[1];
            // totalLines = Integer.parseInt(args[2]);
	    // papi_preset = args[3];
            System.out.println("Usage: java class numberOfThreads filename lineToReads PERSET");
        }


        BufferedReader bis = new BufferedReader(new FileReader(path));
        // ensure data is loaded into memory
        ArrayList<String> lines = new ArrayList<String>();
        for (int i = 0; i < totalLines; i ++) {
            lines.add(bis.readLine());
        }

        int batchSize = 4 * 1048576;
        String executionMode = "cpu";
        int circularBufferSize = 128 * 1 * 1048576/2;
        int unboundedBufferSize = 4 * 1048576;
        int hashTableSize = 2*64*128;
        int partialWindows = 2;
        int slots = 1 * 128 * 1024*2;

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
        SystemConf.hwPerfCounters = papi_preset;
        /* Initialize the Operators of the Benchmark */
        PAPIHardwareSampler [] operators_papi_samplers = new PAPIHardwareSampler[SystemConf.THREADS];

        for (int i = 0; i < operators_papi_samplers.length; i ++) {
            operators_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        }
        PAPIHardwareSampler [] query_papi_samplers = new PAPIHardwareSampler[2];
        for (int i = 0; i < query_papi_samplers.length; i ++) {
            query_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        }

        LinearRoadBenchmarkQuery benchmarkQuery = null;
        benchmarkQuery = new LinearRoadBenchmark (queryConf, true, operators_papi_samplers, query_papi_samplers);
        // zxchen: don't modify this parameter, inside circularbufferworker may use it, not sure.
        int bufferSize = 4 * 131072;
        // int bufferSize = 40 * 1048576;
        // int bufferSize = 128 * 20;
        int numberOfGeneratorThreads = 2;
        int coreToBind = 3; //numberOfThreads + 1;
        PAPIHardwareSampler [] generator_papi_samplers = new PAPIHardwareSampler[numberOfGeneratorThreads];
        for (int i = 0; i < generator_papi_samplers.length; i ++) {
            generator_papi_samplers[i] = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        }

        Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, coreToBind, lines, generator_papi_samplers);
        long timeLimit = System.currentTimeMillis() + 100 * 1000;

        while (true) {
            if (timeLimit <= System.currentTimeMillis()) {
            	System.out.println("Terminating execution...");
                // break;
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


            // if (generator.total_consumed_tuples >= 40*1000000) {
            //     System.out.println("Already consumed tuples: " + generator.total_consumed_tuples);

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
            //     System.exit(0);
            // }

            GeneratedBuffer b = generator.getNext();
            benchmarkQuery.getApplication().processData (b.getBuffer().array());
            b.unlock();
        }
    }
}
