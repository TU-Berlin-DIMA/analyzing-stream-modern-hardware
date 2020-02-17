package uk.ac.imperial.lsds.saber.cql.operators.cpu;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;

public class Selection implements IOperatorCode {

	private IPredicate predicate;

	public Selection (IPredicate predicate) {
		this.predicate = predicate;
	}

	@Override
	public String toString () {
		final StringBuilder s = new StringBuilder();
		s.append("Selection (");
		s.append(predicate.toString());
		s.append(")");
		return s.toString();
	}

	public void processData (WindowBatch batch, IWindowAPI api) {

		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();
        // System.out.print("[DBG] Before, tuple size: " + tupleSize);
        // System.out.println(", batch.getBufferStartPointer(): " + batch.getBufferStartPointer() +
        //                    ", batch.getBufferEndpointer(): " + batch.getBufferEndPointer());
        int qualified = 0;
		for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
            // System.out.print("[DBG] pickup_ts: " + inputBuffer.getLong(pointer + 72));
            // System.out.print(", dropoff_ts: " + inputBuffer.getLong(pointer + 80));
            // System.out.print(", passenger_count: " + inputBuffer.getLong(pointer + 88));
            // System.out.print("[DBG] trip_distance: " + inputBuffer.getDouble(pointer + 104));
            // System.out.print(", pickup_long: " + inputBuffer.getDouble(pointer + 128));
            // System.out.print(", pickup_lang: " + inputBuffer.getDouble(pointer + 136));
            // System.out.println();
            // System.out.println("[DBG] predicate.toString(): " + predicate.toString());
			if (predicate.satisfied (inputBuffer, schema, pointer)) {
				/* Write tuple to result buffer */
                inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
                qualified ++;
			}
		}

		inputBuffer.release();
        // update buffer and pointers inside this batch
		batch.setBuffer(outputBuffer);
        batch.setBufferPointers(0, outputBuffer.position());
		api.outputWindowBatchResult (batch);
        // System.out.println("qualifed tuple in selection is: " + qualified);
	}

	public void processData (WindowBatch first, WindowBatch second, IWindowAPI api) {

		throw new UnsupportedOperationException("error: operator does not operate on two streams");
	}

	public void configureOutput (int queryId) {

		throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
	}

	public void processOutput (int queryId, WindowBatch batch) {

		throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
	}

	public void setup() {

		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
	}
}
