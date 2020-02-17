package uk.ac.imperial.lsds.saber.experiments.benchmarks.nyt;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public abstract class InputStream implements NYTBenchmarkQuery {

	ITupleSchema schema = null;
	QueryApplication application = null;

	public InputStream () {

	}

	public QueryApplication getApplication () {
		return this.application;
	}
	public ITupleSchema getSchema () {
        if (schema == null)
            createSchema();

		return schema;
	}

    public abstract void createApplication (QueryConf queryConf, boolean isExecuted);

	// create schema for SABER
	public void createSchema () {

		int [] offsets = new int [15];
        offsets[0] =  0;        // timestamp
		offsets[1] =  8;        // medallion
		offsets[2] = 38;        // hack_license
		offsets[3] = 68;        // vendor_id
		offsets[4] = 72;        // rate_code_id
		offsets[4] = 80;        // pickup_ts
		offsets[5] = 88;        // dropoff_ts
		offsets[7] = 96;        // passenger_count
		offsets[8] = 104;        // trip_time_in_secs
		offsets[9] = 112;       // trip_distance
		offsets[10] = 120;      // dropoff_long
		offsets[11] = 128;      // dropoff_lang
		offsets[12] = 136;      // pickup_long
		offsets[13] = 144;      // pickup_lang
		offsets[14] = 152;      // store_and_fwd_flag

		schema = new TupleSchema (offsets, 256); // padding 145 to 256

		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
		schema.setAttributeType(0, PrimitiveType.LONG);
		schema.setAttributeType(1, PrimitiveType.VARCHAR);
		schema.setAttributeType(2, PrimitiveType.VARCHAR);
		schema.setAttributeType(3, PrimitiveType.INT);
		schema.setAttributeType(4, PrimitiveType.LONG);
		schema.setAttributeType(5, PrimitiveType.LONG);
		schema.setAttributeType(6, PrimitiveType.LONG);
		schema.setAttributeType(7, PrimitiveType.LONG);
		schema.setAttributeType(8, PrimitiveType.LONG);
		schema.setAttributeType(9, PrimitiveType.DOUBLE);
		schema.setAttributeType(10, PrimitiveType.DOUBLE);
		schema.setAttributeType(11, PrimitiveType.DOUBLE);
		schema.setAttributeType(12, PrimitiveType.DOUBLE);
		schema.setAttributeType(13, PrimitiveType.DOUBLE);
		schema.setAttributeType(14, PrimitiveType.BYTE);

		schema.setAttributeName (0, "tiemstamp"); // timestamp
		schema.setAttributeName (1, "medallion"); // timestamp
		schema.setAttributeName (2, "hack_license");
		schema.setAttributeName (3, "vendor_id");
		schema.setAttributeName (4, "rate_code_id");
		schema.setAttributeName (5, "pickup_ts");
		schema.setAttributeName (6, "dropoff_ts");
		schema.setAttributeName (7, "passenger_count");
		schema.setAttributeName (8, "trip_time_in_secs");
		schema.setAttributeName (9, "trip_distance");
        schema.setAttributeName (10, "dropoff_long");
		schema.setAttributeName (11, "dropoff_lagn");
        schema.setAttributeName (12, "pickup_long");
		schema.setAttributeName (13, "pickup_lang");
		schema.setAttributeName (14, "store_and_fwd_flag");

		schema.setName("New York Taxi");
	}
}
