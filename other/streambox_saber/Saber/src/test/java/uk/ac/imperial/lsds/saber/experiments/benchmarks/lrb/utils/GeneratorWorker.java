package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;
import uk.ac.imperial.lsds.saber.SystemConf;
public class GeneratorWorker implements Runnable {

	Generator generator;
	volatile boolean started = false;

	private int isFirstTime = 2;
	private final int startPos;
	private final int endPos;
	private final int id;

    private BufferedReader reader;
    private String line = null;
    private ArrayList<String> lines;

    private int index = 0;

    public long consumed_tuples = 0;
    private final int tuples_per_buffer;

    private PAPIHardwareSampler hwPAPI;
	public GeneratorWorker (Generator generator, int startPos, int endPos, int id, ArrayList<String> lines, PAPIHardwareSampler papi_sampler) {
		this.generator = generator;
		this.startPos = startPos;
		this.endPos = endPos;
		this.id = id;
        this.lines = lines;
        // this.hwPAPI = new PAPIHardwareSampler(SystemConf.hwPerfCounters);
        this.hwPAPI = papi_sampler;
        this.tuples_per_buffer = (endPos - startPos) / 128;
	}

	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {

	}

	@Override
	public void run() {

		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, id));

		int curr;
		GeneratedBuffer buffer;
		int prev = 0;
		long timestamp;

		started = true;
        try {
            this.hwPAPI.startSampling();
            while (true) {

			while ((curr = generator.next) == prev)
				;

			// System.out.println("Filling buffer " + curr);

			buffer = generator.getBuffer (curr);

			/* Fill buffer... */
			timestamp = generator.getTimestamp ();

            generate(buffer, startPos, endPos, timestamp);

			buffer.decrementLatch ();
			prev = curr;
            this.consumed_tuples += this.tuples_per_buffer;
            // if (this.consumed_tuples >= 20*1000000) {
            //     System.out.println("Already processed " + this.consumed_tuples + " tuples .");
            //     break;
            // }
			// System.out.println("done filling buffer " + curr);
			// break;
		}
            // this.hwPAPI.stopSampling("GeneratorWorker: ");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

		// System.out.println("worker exits " );
	}

	private void generate(GeneratedBuffer generatedBuffer, int startPos, int endPos, long timestamp) {

		ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		/* Fill the buffer */
        buffer.position(startPos);
        while (buffer.position()  < endPos) {
            buffer.putLong (timestamp);
            this.line = this.lines.get(this.index);
            String[] tokens = this.line.split(",");
            for (int i = 0; i < 15; i ++) {
                buffer.putInt(Integer.parseInt(tokens[i]));
            }
            buffer.position(buffer.position() + 60); // padding to 128 bytes

            this.index ++;
            // end of line, reuse the buffer
            if (this.index >= this.lines.size()) {
                this.index = 0;
            }
        }
	}
}
