package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.TupleSchema;
// import uk.ac.imperial.lsds.saber.processors.HashMap;
import java.util.HashMap;
import java.util.Map;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class NYTBenchmarkOp implements IOperatorCode, IAggregateOperator {

	private static final boolean debug = true;

	private static boolean monitorSelectivity = false;

	private long invoked = 0L;
	private long matched = 0L;

	private IQueryBuffer relationBuffer;

	private WindowDefinition windowDefinition;

	private IPredicate selectPredicate = null;

	private Expression [] expressions;

	private ITupleSchema projectedSchema;

	private IPredicate joinPredicate = null;

	private ITupleSchema joinedSchema;

	private ITupleSchema relationSchema;

	private ITupleSchema outputSchema;

	private boolean incrementalProcessing;

	private AggregationType [] aggregationTypes;

	private FloatColumnReference [] aggregationAttributes;

	private LongColumnReference timestampReference;

	private Expression [] groupByAttributes;
	private boolean groupBy = false;

	private int keyLength, valueLength;

	// private boolean isV2 = true;

	/* Thread local variables */
	private ThreadLocal<float   []> tl_values;
	private ThreadLocal<int     []> tl_counts;
	private ThreadLocal<byte    []> tl_tuplekey;
	private ThreadLocal<boolean []> tl_found;

	//private Multimap<Integer,Integer> multimap;
	// private HashMap<Integer, Long> hashMap;
    // private HashMap<Integer, Double> hmap = null;
    private HashMap<Integer, Float> hmap = null;

    public NYTBenchmarkOp() {
        this.hmap = new HashMap<Integer, Float>();
        final int [] offset = new int []{0, 8, 12, 16}; // number of trips, total trip_distance
        final int tupleSize = 32;               // timestamp: long, trips: long, trip_distance: float, region: int
        this.outputSchema = new TupleSchema(offset, tupleSize);
        this.outputSchema.setAttributeType(0, PrimitiveType.LONG); // 8
        this.outputSchema.setAttributeType(1, PrimitiveType.FLOAT); // 4
        // this.outputSchema.setAttributeType(1, PrimitiveType.DOUBLE);
        this.outputSchema.setAttributeType(2, PrimitiveType.FLOAT); // 4
        this.outputSchema.setAttributeType(3, PrimitiveType.INT);   // 4

        this.outputSchema.setAttributeName(0, "timestamp");
        this.outputSchema.setAttributeName(1, "number_of_trips");
        this.outputSchema.setAttributeName(2, "total_trip_distance");
        this.outputSchema.setAttributeName(3, "region");

        this.outputSchema.setName("nyt");
    }

	@Override
	public boolean hasGroupBy () {
		return groupBy;
	}

	@Override
	public ITupleSchema getOutputSchema () {
		return outputSchema;
	}

	@Override
	public int getKeyLength() {
		return keyLength;
	}

	@Override
	public int getValueLength() {
		return valueLength;
	}

	@Override
	public int numberOfValues() {
		return aggregationAttributes.length;
	}

	@Override
	public AggregationType getAggregationType() {
		return getAggregationType (0);
	}

	@Override
	public AggregationType getAggregationType(int idx) {
		if (idx < 0 || idx > aggregationTypes.length - 1)
			throw new ArrayIndexOutOfBoundsException ("error: invalid aggregation type index");
		return aggregationTypes[idx];
	}

	@Override
	public void processData(WindowBatch batch, IWindowAPI api) {
        // System.out.println("Process NYT OP.");
        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();
        // System.out.print("[DBG] Before, tuple size: " + tupleSize);
        // System.out.println(", batch.getBufferStartPointer(): " + batch.getBufferStartPointer() +
        //                    ", batch.getBufferEndpointer(): " + batch.getBufferEndPointer());
        // System.out.print("[DBG] ");
        int qualified = 0;
        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
            float trip_distance = (float)inputBuffer.getDouble(pointer+112);
            // if (trip_distance > 5.0) {
            if (trip_distance < 5.0) {
                System.out.println("[ERROR]");
                System.exit(1);
            }
            int vendorId = inputBuffer.getInt(pointer+68); // extract vendorID;
            byte[] vendor_id = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(vendorId).array();
            if (vendor_id[0] == 86) // 'V'
            {
                // System.out.println("[DBG] vendorId: " + vendorId + ", vendor_id: " + new String(vendor_id));
                qualified ++;
                long longitude = (long)((inputBuffer.getDouble(pointer+136) + 750) * 100); // pickup_long
                long latitude = (long)((inputBuffer.getDouble(pointer+144) + 1400) * 100); // pickup_lang
                int region = (int)((longitude + 12345*latitude) % 100);
                if (this.hmap.containsKey(region)) {
                    float _trip_distance = this.hmap.get(region);
                    _trip_distance += trip_distance;
                    this.hmap.put(region, _trip_distance);
                } else {
                    this.hmap.put(region, trip_distance);
                }
                // System.out.print("region: " + region);
                // System.out.print(", pickup_long: " + inputBuffer.getDouble(pointer+128));
                // System.out.print(", pickup_lang: " + inputBuffer.getDouble(pointer+136));
                // System.out.print(", long: " + longitude);
                // System.out.print(", lang: " + latitude);
                // System.out.print(", trip_distance: " + trip_distance);
                // System.out.println();
            }
        }
        int key = 0;
        float val = 0;
        for (Map.Entry<Integer, Float> entry : this.hmap.entrySet()) {
            // int key = entry.getKey();
            // float val = entry.getValue();
            key = entry.getKey();
            val = entry.getValue();

        }
        // System.out.println("[DBG] region: " + key + ", trip_distance: " + val + ", qualified: " + qualified);
        for (int i = 0; i < 1; i ++) {
            outputBuffer.putLong(0L); // timestamp
            outputBuffer.putFloat((float)qualified); // number of qualified trips
            // outputBuffer.putDouble(val);
            outputBuffer.putFloat(val); // trip_distance;
            outputBuffer.putInt(key); // region
            outputBuffer.putLong(0L); // padding
            outputBuffer.putInt(0); // padding
        }
        inputBuffer.release();
        // outputBuffer.close();
        batch.setBuffer(outputBuffer);
        batch.setBufferPointers(0, outputBuffer.position());
        api.outputWindowBatchResult(batch);
        // System.out.print("[DBG] After");
        // System.out.println(", batch.getBufferStartPointer(): " + batch.getBufferStartPointer() +
        //                    ", batch.getBufferEndpointer(): " + batch.getBufferEndPointer());

    }

	private void calc(WindowBatch batch, IWindowAPI api) {
		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();

		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

			if (selectPredicate.satisfied (inputBuffer, schema, pointer)) {

				/* Write tuple to result buffer */
				for (int i = 0; i < expressions.length; ++i) {

					expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
				}
				outputBuffer.put(projectedSchema.getPad());			}
		}

		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();

		/* Reset position for output buffer */
		outputBuffer.close();

		/* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
		batch.setBuffer(outputBuffer);
		batch.setSchema(projectedSchema);

		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());

		api.outputWindowBatchResult (batch);
	}

	private void select(WindowBatch batch, IWindowAPI api) {

		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();

		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

			if (selectPredicate.satisfied (inputBuffer, schema, pointer)) {

				/* Write tuple to result buffer */
				inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
			}
		}

		inputBuffer.release();

		/* Reset position for output buffer */
		outputBuffer.close();

		batch.setBuffer(outputBuffer);

		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());

		api.outputWindowBatchResult (batch);
	}

	private void project (WindowBatch batch, IWindowAPI api) {

		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
		int tupleSize = schema.getTupleSize();

		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {

			for (int i = 0; i < expressions.length; ++i) {

				expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
			}
			outputBuffer.put(projectedSchema.getPad());
		}

		/* Return any (unbounded) buffers to the pool */
		inputBuffer.release();

		/* Reset position for output buffer */
		outputBuffer.close();

		/* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
		batch.setBuffer(outputBuffer);
		batch.setSchema(projectedSchema);

		/* Important to set start and end buffer pointers */
		batch.setBufferPointers(0, outputBuffer.limit());

		api.outputWindowBatchResult (batch);
	}

// 	private void hashJoin (WindowBatch batch, IWindowAPI api) {

// 		IQueryBuffer inputBuffer = batch.getBuffer();
// 		byte[] bInputBuffer = inputBuffer.getByteBuffer().array();
// 		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

// 		int column1 = isV2? ((LongColumnReference)joinPredicate.getFirstExpression()).getColumn() :
// 								((LongLongColumnReference)joinPredicate.getFirstExpression()).getColumn();
// 		int offset1 = projectedSchema.getAttributeOffset(column1);
// 		int currentIndex1 =  batch.getBufferStartPointer();
// 		int currentIndex2 =  0;// relationBuffer.getBufferStartPointer();

// 		int endIndex1 = batch.getBufferEndPointer() + 32;
// 		int endIndex2 = relationBuffer.limit();//relationBatch.getBufferEndPointer() + 32;

// 		int tupleSize1 = projectedSchema.getTupleSize();
// 		int tupleSize2 = relationSchema.getTupleSize();

// 		/* Actual Tuple Size without padding*/
// 		int pointerOffset1 = tupleSize1 - projectedSchema.getPadLength();
// 		int pointerOffset2 = tupleSize2 - relationSchema.getPadLength();

// 		if (monitorSelectivity)
// 			invoked = matched = 0L;

// 		/* Is one of the windows empty? */
// 		if (currentIndex1 != endIndex1 && currentIndex2 != endIndex2) {

// 			byte [] key = isV2? new byte[8] : new byte[16];
// 			ByteBuffer b = ByteBuffer.wrap(key);
// 			int value;
// 			int j = 0;

// 			for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize1) {

// 				if (monitorSelectivity)
// 					invoked ++;

// 				/*while (j < key.length) {
// 					key[j] = bInputBuffer[pointer + offset1 + j];
// 					j += 1;
// 				}
// 				j = 0;*/

// 				System.arraycopy(inputBuffer.array(), pointer + offset1 + j, b.array(), 0,key.length);



// 				value = hashMap.get(key);
// 				if (value != -1) {
// 					// Write tuple to result buffer
// 					inputBuffer.appendBytesTo(pointer, pointerOffset1, outputBuffer);
// 					relationBuffer.appendBytesTo(value, pointerOffset2, outputBuffer);

// 					/* Write dummy content, if needed */
// 					outputBuffer.put(this.joinedSchema.getPad());

// 					if (monitorSelectivity)
// 						matched ++;
// 				}
// 			}
// 		}

// 		if (debug)
// 			System.out.println("[DBG] output buffer position is " + outputBuffer.position());

// 		if (monitorSelectivity) {
// 			double selectivity = 0D;
// 			if (invoked > 0)
// 				selectivity = ((double) matched / (double) invoked) * 100D;
// 			System.out.println(String.format("[DBG] task %6d %2d out of %2d tuples selected (%4.1f)",
// 					batch.getTaskId(), matched, invoked, selectivity));
// 		}

// 		/*		Print tuples */
// /*		outputBuffer.close();
// 		int tid = 1;
// 		while (outputBuffer.hasRemaining()) {

// 			System.out.println(String.format("%03d: %2d,%2d,%2d | %2d,%2d,%2d",
// 			tid++,
// 		    outputBuffer.getByteBuffer().getLong(),
// 			outputBuffer.getByteBuffer().getInt (),
// 			outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			outputBuffer.getByteBuffer().getLong(),
// 			outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			//outputBuffer.getByteBuffer().getInt (),
// 			outputBuffer.getByteBuffer().getInt ()
// 			));
// 		}
// 		System.err.println("Disrupted");
// 		System.exit(-1);*/

// 		/* Return any (unbounded) buffers to the pool */
// 		inputBuffer.release();

// 		/* Reset position for output buffer */
// 		//outputBuffer.close();

// 		batch.setBuffer(outputBuffer);
// 		batch.setSchema(joinedSchema);

// 		/* Important to set start and end buffer pointers */
// 		//batch.setBufferPointers(0, outputBuffer.limit());

// 		api.outputWindowBatchResult (batch);
// 	}

	private void setGroupByKey (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
		int pivot = 0;
		for (int i = 0; i < groupByAttributes.length; i++) {
			pivot = groupByAttributes[i].evalAsByteArray (buffer, schema, offset, bytes, pivot);
		}
	}

	private void processDataPerWindowWithGroupBy (WindowBatch batch, IWindowAPI api) {

		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());

		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();

		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();

		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);

		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer;

		/* Current window start and end pointers */
		int start, end;

		WindowHashTable windowHashTable;
		byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
		boolean [] found = (boolean []) tl_found.get(); // new boolean[1];
		boolean pack = false;

		float [] values = tl_values.get();

		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;

			pack = false;

			start = startP [currentWindow];
			end   = endP   [currentWindow];

			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;
				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
				pack = true;
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;

			windowHashTable = WindowHashTableFactory.newInstance(workerId);
			windowHashTable.setTupleLength(keyLength, valueLength);

			while (start < end) {
				/* Get the group-by key */
				setGroupByKey (inputBuffer, inputSchema, start, tupleKey);
				/* Get values */
				for (int i = 0; i < numberOfValues(); ++i) {
					if (aggregationTypes[i] == AggregationType.CNT)
						values[i] = 1;
					else {
						if(inputBuffer == null)
							System.err.println("Input is iull?");
						if(inputSchema == null)
							System.err.println("Schema is null?");
						if(values == null)
							System.err.println("Values is null?");
						if(aggregationAttributes[i] == null)
							System.err.println("Attributes is null?");
						values[i] = aggregationAttributes[i].eval (inputBuffer, inputSchema, start);

					}
				}

				/* Check whether there is already an entry in the hash table for this key.
				 * If not, create a new entry */
				found[0] = false;
				int idx = windowHashTable.getIndex (tupleKey, found);
				if (idx < 0) {
					System.out.println("error: open-adress hash table is full");
					System.exit(1);
				}

				ByteBuffer theTable = windowHashTable.getBuffer();
				if (! found[0]) {
					theTable.put (idx, (byte) 1);
					int timestampOffset = windowHashTable.getTimestampOffset (idx);
					theTable.position (timestampOffset);
					/* Store timestamp */
					theTable.putLong (inputBuffer.getLong(start));
					/* Store key and value(s) */
					theTable.put (tupleKey);
					for (int i = 0; i < numberOfValues(); ++i)
						theTable.putFloat(values[i]);
					/* Store count */
					theTable.putInt(1);
				} else {
					/* Update existing entry */
					int valueOffset = windowHashTable.getValueOffset (idx);
					int countOffset = windowHashTable.getCountOffset (idx);
					/* Store value(s) */
					float v;
					int p;
					for (int i = 0; i < numberOfValues(); ++i) {
						p = valueOffset + i * 4;
						switch (aggregationTypes[i]) {
						case CNT:
							theTable.putFloat(p, (theTable.getFloat(p) + 1));
							break;
						case SUM:
						case AVG:
							theTable.putFloat(p, (theTable.getFloat(p) + values[i]));
						case MIN:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v > values[i]) ? values[i] : v));
							break;
						case MAX:
							v = theTable.getFloat(p);
							theTable.putFloat(p, ((v < values[i]) ? values[i] : v));
							break;
						default:
							throw new IllegalArgumentException ("error: invalid aggregation type");
						}
					}
					/* Increment tuple count */
					theTable.putInt(countOffset, theTable.getInt(countOffset) + 1);
				}
				/* Move to next tuple in window */
				start += inputTupleSize;
			}
			/* Store window result and move to next window */
			evaluateWindow (windowHashTable, outputBuffer, pack);
			/* Release hash maps */
			windowHashTable.release();
		}

		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}

	private void evaluateWindow (WindowHashTable windowHashTable, IQueryBuffer buffer, boolean pack) {

		/* Write current window results to output buffer; copy the entire hash table */
		if (! pack) {
			buffer.put(windowHashTable.getBuffer().array());
			return;
		}

		/* Set complete windows */
/*		System.out.println("Complete windows start at " + buffer.position());
*/
		ByteBuffer theTable = windowHashTable.getBuffer();
		int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
		/* Pack the elements of the table */

/*		int tupleIndex = 0;
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
			if (theTable.get(idx) == 1) {
				int mark = theTable.get(idx);
				long timestamp = theTable.getLong(idx + 8);
				long fKey = theTable.getLong(idx + 16);
				long key = theTable.getLong(idx + 24);
				float val1 = theTable.getFloat(idx + 32);
				float val2 = theTable.getFloat(idx + 36);
				int count = theTable.getInt(idx + 40);
				System.out.println(String.format("%5d: %10d, %10d, %10d, %10d, %5.3f, %5.3f, %10d",
						tupleIndex,
						mark,
						timestamp,
						fKey,
						key,
						val1,
						val2,
						count
						));
			}
			tupleIndex ++;
		}*/

		//System.exit(1);
//		int tupleIndex = 0;
//		for (int idx = offset; idx < (offset + SystemConf.HASH_TABLE_SIZE); idx += 32) {
//			int mark = buffer.getInt(idx + 0);
//			if (mark > 0) {
//				long timestamp = buffer.getLong(idx + 8);
//				//
//				// int key_1
//				// float value1
//				// float value2
//				// int count
//				//
//				int key = buffer.getInt(idx + 16);
//				float val1 = buffer.getFloat(idx + 20);
//				float val2 = buffer.getFloat(idx + 24);
//				int count = buffer.getInt(idx + 28);
//				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d",
//					tupleIndex,
//					Integer.reverseBytes(mark),
//					Long.reverseBytes(timestamp),
//					Integer.reverseBytes(key),
//					0F,
//					0F,
//					Integer.reverseBytes(count)
//				));
//			}
//			tupleIndex ++;
//		}

		// ByteBuffer theTable = windowHashTable.getBuffer();
		// int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();

		/* Pack the elements of the table */
		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {

			if (theTable.get(idx) != 1) /* Skip empty slots */
				continue;

			/* Store timestamp, and key */
			int timestampOffset = windowHashTable.getTimestampOffset (idx);
			buffer.put(theTable.array(), timestampOffset, (8 + keyLength));

			int valueOffset = windowHashTable.getValueOffset (idx);
			int countOffset = windowHashTable.getCountOffset (idx);

			int count = theTable.getInt(countOffset);
			int p;
			for (int i = 0; i < numberOfValues(); ++i) {
				p = valueOffset + i * 4;
				if (aggregationTypes[i] == AggregationType.AVG) {
					buffer.putFloat(theTable.getFloat(p) / (float) count);
				} else {
					buffer.putFloat(theTable.getFloat(p));
				}
			}
			buffer.put(outputSchema.getPad());
		}
	}

	private void processDataPerWindowIncrementallyWithGroupBy (WindowBatch batch, IWindowAPI api) {

		throw new UnsupportedOperationException("error: operator does not support incremental computation yet");
	}

	@Override
	public void processData(WindowBatch first, WindowBatch second, IWindowAPI api) {

		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}

	@Override
	public void configureOutput (int queryId) {

		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	@Override
	public void processOutput(int queryId, WindowBatch batch) {

		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}

	@Override
	public void setup() {

		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}

}
