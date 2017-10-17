package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;

@ScalarFunction("array_bucket")
@Description("Find the bucket to which the argument belongs.")
public class ArrayBucketFunction
{
    private ArrayBucketFunction(){};

    @TypeParameter("E")
    @SqlType("integer")
    @SqlNullable
    public static Long bucket(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block bucket,
            @SqlNullable @SqlType("double") Double value)
    {
        BlockBuilder v = type.createBlockBuilder(null, 1);
        if (value == null) {
            return null;
        }
        int bucketCount = bucket.getPositionCount();

        for (int ii = 0; ii < bucketCount; ++ii) {
            Double c = type.getDouble(bucket, ii);
            if (c != null && value <= c) {
                return Long.valueOf(ii);
            }
        }
        return new Long(bucketCount);
    }
}
