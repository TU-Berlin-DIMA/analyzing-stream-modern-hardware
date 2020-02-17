package uk.ac.imperial.lsds.saber.experiments.benchmarks.nyt;
import java.io.FileInputStream;
import java.io.DataInputStream;
// import java.io.LittleEndianDataInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.FileNotFoundException;
import java.io.IOException;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;


public class NYTBenchmarkApp {
	// public static final String usage = "usage: YahooBenchmarkApp with in-memory generation";

	public static void main (String [] args) throws InterruptedException {
        // SystemConf.UNBOUNDED_BUFFER_SIZE = 128 * 1024 * 1024; // numberOfBatch = unbounded_buffer_size / batchSize
        SystemConf.CIRCULAR_BUFFER_SIZE = 1024 * 1024 * 1024;
		SystemConf.THREADS = 8;
        SystemConf.PARTIAL_WINDOWS = 2;
        String path = "/home/zxchen/nydata/data.bin";
        DataInputStream dataIn = null;
        try {
            dataIn = new DataInputStream(new FileInputStream(path));
        } catch (FileNotFoundException ex) {
            System.out.println("[ERROR] File " + path + " not found.");
            System.exit(1);
        }
        final int bufferSize = 512*1024*1024;
        IQueryBuffer buffer = new UnboundedQueryBuffer(Integer.MAX_VALUE, bufferSize, false);
        final int totalTuples = 2*1024*1024;
        // final int totalTuples = 100;
        final int tupleSize = 256;
        int cnt = 0;

        while (cnt < totalTuples) {
            try {
                byte [] read = new byte[tupleSize-8];
                long timestamp = 0L;
                buffer.putLong(timestamp);
                dataIn.readFully(read, 0, 145);
                buffer.put(read);
                // byte[] medallion = new byte[30];
                // byte[] hack_license = new byte[30];
                // byte[] vendor_id = new byte[4];
                // // long
                // byte[] rate_code_id = new byte[8];
                // byte[] pickup_ts = new byte[8];
                // byte[] dropoff_ts  = new byte[8];
                // byte[] passenger_count= new byte[8];
                // byte[] trip_time_in_secs = new byte[8];
                // // double
                // byte[] trip_distance = new byte[8];
                // byte[] dropoff_long = new byte[8];
                // byte[] dropoff_lang  = new byte[8];
                // byte[] pickup_long = new byte[8];
                // byte[] pickup_lang = new byte[8];
                // byte [] store_and_fwd_flag = new byte[1];

                // ByteBuffer medallion_buffer = ByteBuffer.wrap(medallion);
                // medallion_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer hack_license_buffer = ByteBuffer.wrap(hack_license);
                // hack_license_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer vendor_id_buffer = ByteBuffer.wrap(vendor_id);
                // vendor_id_buffer.order(ByteOrder.LITTLE_ENDIAN);

                // ByteBuffer rate_code_id_buffer = ByteBuffer.wrap(rate_code_id);
                // rate_code_id_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer pickup_ts_buffer = ByteBuffer.wrap(pickup_ts);
                // pickup_ts_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer dropoff_ts_buffer = ByteBuffer.wrap(dropoff_ts);
                // dropoff_ts_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer passenger_count_buffer = ByteBuffer.wrap(passenger_count);
                // passenger_count_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer trip_time_in_secs_buffer = ByteBuffer.wrap(trip_time_in_secs);
                // trip_time_in_secs_buffer.order(ByteOrder.LITTLE_ENDIAN);

                // ByteBuffer trip_distance_buffer = ByteBuffer.wrap(trip_distance);
                // trip_distance_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer dropoff_long_buffer = ByteBuffer.wrap(dropoff_long);
                // dropoff_long_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer dropoff_lang_buffer = ByteBuffer.wrap(dropoff_lang);
                // dropoff_lang_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer pickup_long_buffer = ByteBuffer.wrap(pickup_long);
                // pickup_long_buffer.order(ByteOrder.LITTLE_ENDIAN);
                // ByteBuffer pickup_lang_buffer = ByteBuffer.wrap(pickup_lang);
                // // pickup_lang_buffer.order(ByteOrder.LITTLE_ENDIAN);

                // dataIn.readFully(medallion, 0, 30);
                // dataIn.readFully(hack_license, 0, 30);
                // dataIn.readFully(vendor_id, 0, 4);
                // dataIn.readFully(rate_code_id, 0, 8);
                // dataIn.readFully(pickup_ts, 0, 8);
                // dataIn.readFully(dropoff_ts, 0, 8);
                // dataIn.readFully(passenger_count, 0, 8);
                // dataIn.readFully(trip_time_in_secs, 0, 8);
                // dataIn.readFully(trip_distance, 0, 8);
                // dataIn.readFully(dropoff_long, 0, 8);
                // dataIn.readFully(dropoff_lang, 0, 8);
                // dataIn.readFully(pickup_long, 0, 8);
                // dataIn.readFully(pickup_lang, 0, 8);

                // // long rate_code_id = dataIn.readLong();
                // // long pickup_ts = dataIn.readLong();
                // // long dropoff_ts = dataIn.readLong();
                // // long passenger_count = dataIn.readLong();
                // // long trip_time_in_secs = dataIn.readLong();

                // // double trip_distance = dataIn.readDouble();
                // // double dropoff_long = dataIn.readDouble();
                // // double dropoff_lang = dataIn.readDouble();
                // // double pickup_long = dataIn.readDouble();
                // // double pickup_lang = dataIn.readDouble();
                // dataIn.readFully(store_and_fwd_flag, 0, 1);
                // // byte store_and_fwd_flag = dataIn.readByte();
                // String medallion_str = medallion_buffer.toString();
                // String hack_license_str = hack_license_buffer.toString();
                // String vendor_id_str = vendor_id_buffer.toString();
                // byte[] medallion_bytes = medallion_buffer.array();
                // byte[] pickup_lang_bytes = pickup_lang_buffer.array();

                // for (int i = 0; i < medallion.length; i ++) {
                //     System.out.println(medallion[i] + ", " + medallion_bytes[i]);
                // }
                // System.out.println();
                // for (int i = 0; i < pickup_lang.length; i ++) {
                //     System.out.println(pickup_lang[i] + ", " + pickup_lang_bytes[i]);
                // }

                // System.out.println();
                // System.out.println("medallion: " + medallion_str + ", hack_license: " + hack_license_str +
                //                    ", vendor_id: " + vendor_id_str +
                //                    ", rate_code_id: " + rate_code_id_buffer.getLong() + ", pickup_ts: " + pickup_ts_buffer.getLong() +
                //                    ", dropoff_ts: " + dropoff_ts_buffer.getLong() + ", passenger_count: " + passenger_count_buffer.getLong() +
                //                    ", trip_time_in_secs: " + trip_time_in_secs_buffer.getLong() +
                //                    ", trip_distance: " + trip_distance_buffer.getDouble() + ", dropoff_long: " + dropoff_long_buffer.getDouble() +
                //                    ", dropoff_lang: " + dropoff_lang_buffer.getDouble() + ", pickup_long: " + pickup_long_buffer.getDouble() +
                //                    ", pickup_lang: " + pickup_lang_buffer.getDouble() + ", store_and_fwd_flag: " + new String(store_and_fwd_flag));
                cnt ++;
            } catch (IOException ex) {

            }
        }
        System.out.println("[DBG] buffer.position: " + buffer.position());
        for (int i = 0; i < 2; i ++) {
            System.out.print("[DBG] pickup_ts: " + buffer.getLong(i * tupleSize + 80));
            System.out.print(", dropoff_ts: " + buffer.getLong(i * tupleSize + 88));
            System.out.print(", passenger_count: " + buffer.getLong(i * tupleSize + 96));
            System.out.print(", pickup_long: " + buffer.getDouble(i * tupleSize + 136));
            System.out.print(", pickup_lang: " + buffer.getDouble(i * tupleSize + 144));
            System.out.print(", timestamp: " + buffer.getLong(i * tupleSize + 0));
            System.out.println();
        }

        int batchSize = 4 * 1024*1024;
        QueryConf queryConf = new QueryConf (batchSize);

        NYTBenchmarkQuery nyt_query = new NYTBenchmark(queryConf, false);

        String schema = nyt_query.getSchema().getSchema();
        System.out.println(schema);
        long timeLimit = System.currentTimeMillis() + 10 * 1000;

        while (true) {
            // if (timeLimit <= System.currentTimeMillis()) {
            //     break;
            // }
            nyt_query.getApplication().processData(buffer.getByteBuffer().array());
            // System.out.println("position: " + buffer.position());
        }
    }
}
