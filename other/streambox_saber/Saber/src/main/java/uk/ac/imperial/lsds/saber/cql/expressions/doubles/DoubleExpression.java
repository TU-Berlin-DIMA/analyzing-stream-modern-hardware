package uk.ac.imperial.lsds.saber.cql.expressions.doubles;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;

public interface DoubleExpression extends Expression {
	public double eval (IQueryBuffer buffer, ITupleSchema schema, int offset);
}
