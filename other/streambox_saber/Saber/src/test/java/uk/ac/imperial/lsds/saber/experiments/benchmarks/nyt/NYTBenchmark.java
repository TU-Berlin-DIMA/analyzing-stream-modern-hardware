package uk.ac.imperial.lsds.saber.experiments.benchmarks.nyt;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.doubles.DoubleConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.doubles.DoubleColumnReference;
import uk.ac.imperial.lsds.saber.cql.predicates.DoubleComparisonPredicate;

import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NYTBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.processors.HashMap;


public class NYTBenchmark extends InputStream {

    public NYTBenchmark(QueryConf queryConf, boolean isExecuted) {
        this.createSchema();
        this.createApplication(queryConf, isExecuted);
    }
    public void createApplication(QueryConf queryConf, boolean isExecuted) {
		long timestampReference = System.nanoTime();
		boolean realtime = true;
        int windowSize = 2000;
        // tumbling window
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

        // ITupleSchema inputSchema = this.schema;
        /* FILTER (trip_distance > 5) */
        IPredicate select_predicate = new DoubleComparisonPredicate
            (DoubleComparisonPredicate.GREATER_OP, new DoubleColumnReference(9), new DoubleConstant(5.0));
        // IPredicate select_predicate = new LongComparisonPredicate
        //     (LongComparisonPredicate.GREATER_OP, new LongColumnReference(6), new LongConstant(1));

        IOperatorCode selection_code = new Selection((IPredicate) select_predicate);
        QueryOperator sel_op = new QueryOperator(selection_code, null);
        Set<QueryOperator> operator1 = new HashSet<QueryOperator>();
        operator1.add(sel_op);
        Query query1 = new Query(0, operator1, this.schema, windowDefinition, null, null, queryConf);
        query1.setName("selection");

        IOperatorCode nyt_code = new NYTBenchmarkOp();
        QueryOperator nyt_op = new QueryOperator(nyt_code, null);
        Set<QueryOperator> operator2 = new HashSet<QueryOperator>();
        operator2.add(nyt_op);
        Query query2 = new Query(1, operator2, this.schema, windowDefinition, null, null, queryConf);
        query2.setName("nyt_query");

        ITupleSchema outputSchema = ((NYTBenchmarkOp) nyt_code).getOutputSchema(); // timestamp, number_of_trip, trip_distance, region
        // ITupleSchema outputSchema = this.schema;
		AggregationType [] aggregationTypes = new AggregationType []{AggregationType.SUM, AggregationType.AVG};
        // DoubleColumnReference[] aggregationAttributes = new DoubleColumnReference [1]
        // aggregationAttributes[0] = new DoubleColumnReference(1);;
        FloatColumnReference[] aggregationAttributes = new FloatColumnReference [2];
		aggregationAttributes[0] = new FloatColumnReference(1); // number_of_trips
		aggregationAttributes[1] = new FloatColumnReference(2); // trip_distance
        // Expression [] groupByAttributes = new Expression[] {new DoubleColumnReference(1)};
        Expression [] groupByAttributes = new Expression[] {new IntColumnReference(3)}; // region

        IOperatorCode aggregation_code = new Aggregation(windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
        QueryOperator aggregation_op = new QueryOperator(aggregation_code, null);
        Set<QueryOperator> operator3 = new HashSet<>();
        operator3.add(aggregation_op);
        Query query3 = new Query(2, operator3, outputSchema, windowDefinition, null, null, queryConf);
        query3.setName("agrregation");

        query1.connectTo(query2);
        query2.connectTo(query3);

        Set<Query> queries = new HashSet<Query>();
        queries.add(query1);
        queries.add(query2);
        queries.add(query3);

        this.application = new QueryApplication(queries);
        this.application.setup();
        if (SystemConf.CPU) {
            query3.setAggregateOperator((IAggregateOperator) aggregation_code);
        } else {
            System.out.println("Only support CPU mode.");
            System.exit(1);
        }
    }
}
