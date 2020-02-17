package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public abstract class InputStream implements LinearRoadBenchmarkQuery {

	ITupleSchema schema = null;
	QueryApplication application = null;

	public InputStream () {
	}

	public QueryApplication getApplication () {
		return application;
	}

	public abstract void createApplication (QueryConf queryConf, boolean isExecuted);
	// public abstract void createApplication (QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns);

	public ITupleSchema getSchema () {
		if (schema == null) {
            createSchema ();
		}
		return schema;
	}

	// create schema for SABER
	public void createSchema () {
        final int COLUMNS = 16;
		int [] offsets = new int [COLUMNS]; // one slot for timestamp

        // m_iType, m_iTime, m_iVid, m_iSpeed, m_iXway, m_iLane
        // m_iDir, m_iSeg, m_iPos, m_iQid, m_iSinit, m_iSend
        // m_iDow, m_iTod, m_iDay
        offsets[0] = 0;         // timestamp long
        for (int i = 1; i < 16; i ++) {
            offsets[i] =  8 + 4*(i-1);
        }

		schema = new TupleSchema (offsets, 68);

		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
        schema.setAttributeType (0, PrimitiveType.LONG);
        for (int i = 1; i < COLUMNS; i ++) {
            schema.setAttributeType (i, PrimitiveType.INT);
        }

		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "m_iType");
		schema.setAttributeName (2, "m_iTime");
		schema.setAttributeName (3, "m_iVid");
		schema.setAttributeName (4, "m_iSpeed");
		schema.setAttributeName (5, "m_iXway");
		schema.setAttributeName (6, "m_iLane");
		schema.setAttributeName (7, "m_iDir");
		schema.setAttributeName (8, "m_iSeg");
		schema.setAttributeName (9, "m_iPos");
		schema.setAttributeName (10, "m_iQid");
		schema.setAttributeName (11, "m_iSinit");
		schema.setAttributeName (12, "m_iSend");
		schema.setAttributeName (13, "m_iDow");
		schema.setAttributeName (14, "m_iTod");
		schema.setAttributeName (15, "m_iDay");
		//schema.setName("LinearRoad");
	}
}
