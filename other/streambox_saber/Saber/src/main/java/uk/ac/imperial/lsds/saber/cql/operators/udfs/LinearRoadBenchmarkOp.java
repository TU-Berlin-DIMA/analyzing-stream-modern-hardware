package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;

import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.StopTuple;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.AvgSpeed;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.Accident;

public class LinearRoadBenchmarkOp implements IOperatorCode {

    private ConcurrentHashMap<Integer, Accident> accidents;
    private ConcurrentHashMap<Integer, AvgSpeed> avgSpeed;
    private HashMap<Integer, StopTuple> stopMap;

    public LinearRoadBenchmarkOp (
        ConcurrentHashMap<Integer, Accident> acc,
        ConcurrentHashMap<Integer, AvgSpeed> avgSpeed,
        HashMap<Integer, StopTuple> stopMap
        ) {

        this.accidents = acc;
        this.avgSpeed = avgSpeed;
        this.stopMap = stopMap;
    }

    @Override
	public void processData(WindowBatch batch, IWindowAPI api) {
        // System.out.println("Process LRB OP");
        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
        int tuple_size = schema.getTupleSize();

        int segMin = Integer.MAX_VALUE;
        int segMax = Integer.MIN_VALUE;
        int xwayMin = Integer.MAX_VALUE;
        int xwayMax = Integer.MIN_VALUE;

        int timeOfLastToll = -1;

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tuple_size) {
            // parser tuples
            final long timestamp = inputBuffer.getLong(pointer);
            final int m_iType = inputBuffer.getInt(pointer+8);
            final int m_iTime = inputBuffer.getInt(pointer+12);
            final int m_iVid  = inputBuffer.getInt(pointer+16);
            final int m_iSpeed = inputBuffer.getInt(pointer+20);
            final int m_iXway = inputBuffer.getInt(pointer+24);
            final int m_iLane = inputBuffer.getInt(pointer+28);
            final int m_iDir = inputBuffer.getInt(pointer+32);
            final int m_iSeg = inputBuffer.getInt(pointer+36);
            final int m_iPos = inputBuffer.getInt(pointer+40);
            final int m_iQid = inputBuffer.getInt(pointer+44);
            final int m_iSinit = inputBuffer.getInt(pointer+48);
            final int m_iSend = inputBuffer.getInt(pointer+52);
            final int m_iDow = inputBuffer.getInt(pointer+56);
            final int m_iTod = inputBuffer.getInt(pointer+60);
            final int m_iDay = inputBuffer.getInt(pointer+64);


            boolean possibleAccident = false;
            segMin = Math.min(segMin, m_iSeg);
            segMax = Math.max(segMax, m_iSeg);

            xwayMin = Math.min(xwayMin, m_iXway);
            xwayMax = Math.max(xwayMax, m_iXway);

            if (m_iSpeed == 0) {
                if (stopMap.containsKey(m_iVid)) {
                    StopTuple curr = stopMap.get(m_iVid);
                    if (curr.pos == m_iPos) {
                        curr.count ++;
                        if (curr.count == 4) {
                            possibleAccident = true;
                        }
                    } else {
                        stopMap.put(m_iVid, new StopTuple(m_iPos, 1));
                    }
                }
            }


            if (possibleAccident) {
                // signal accident
                int k = Integer.hashCode(m_iXway) * 31 + m_iPos;
                this.accidents.compute(k, new BiFunction<Integer, Accident, Accident>() {
                        @Override
                        public Accident apply(Integer xway, Accident accident) {
                            if (accident == null) {
                                return new Accident(m_iVid, -1, m_iTime);
                            } else {
                                if (accident.vid2 == -1) {
                                    accident.vid2 = m_iVid;
                                } else if (accident.vid1 == -1) {
                                    accident.vid1 = m_iVid;
                                }
                            }
                            return accident;
                        }
                    });
            }

            if (m_iSpeed > 0) {
                int k = Integer.hashCode(m_iXway) * 31 + m_iPos;
                if (this.accidents.containsKey(k)) {
                    Accident a = this.accidents.get(k);
                    if (a.vid1 == m_iVid) {
                        a.vid1 = -1;
                    } else if (a.vid2 == m_iVid) {
                        a.vid2 = -1;
                    }
                }
            }

            int k = Integer.hashCode(m_iXway) * 31 + m_iSeg;
            this.avgSpeed.computeIfPresent(k, new BiFunction<Integer, AvgSpeed, AvgSpeed>() {
                    @Override
                    public AvgSpeed apply(Integer integer, AvgSpeed avgSpeed) {
                        avgSpeed.count ++;
                        avgSpeed.speed += m_iSpeed;
                        return avgSpeed;
                    }
                });
            avgSpeed.putIfAbsent(k, new AvgSpeed(m_iSpeed, 1));


            if (m_iTime % 300 == 0 && m_iTime > 0 && timeOfLastToll != m_iTime) {

                for (int seg = segMin; seg < segMax; seg++) {

                    int ks = Integer.hashCode(m_iXway) * 31 + seg;

                    if (avgSpeed.containsKey(ks)) {
                        AvgSpeed avg = avgSpeed.get(ks);
                        double averageSpeed = 0;
                        if (avg.count > 0) {
                            averageSpeed = avg.speed / avg.count;
                        }
                        if (averageSpeed > 40) {
                            double tollAmount = 0;
                            if (accidents.containsKey(ks)) {
                                tollAmount = (2 * avg.count) ^ 2;
                            }
                        }
                    }

                }

                timeOfLastToll = m_iTime;
            }
        }

        // System.out.println("StopMap.size: " + this.stopMap.size());
        // System.out.println("Accidents.size: " + this.accidents.size());
        // System.out.println("AvgSpeed.size: " + this.avgSpeed.size());
        inputBuffer.release();
        // outputBuffer.close();
        batch.setBuffer(outputBuffer);
        // batch.setSchema(schema);
		// batch.setBufferPointers(0, outputBuffer.limit());
        api.outputWindowBatchResult(batch);
    }

    @Override
	public void processData(WindowBatch first, WindowBatch second, IWindowAPI api) {
        throw new UnsupportedOperationException("error: operator does not operate on two streams");
    }

    @Override
	public void configureOutput(int queryId) {
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
