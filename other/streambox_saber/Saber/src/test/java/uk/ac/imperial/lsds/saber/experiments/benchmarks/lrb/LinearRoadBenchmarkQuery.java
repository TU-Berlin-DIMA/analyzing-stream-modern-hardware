package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;

public interface LinearRoadBenchmarkQuery {

	public QueryApplication getApplication ();

	public ITupleSchema getSchema ();
}
