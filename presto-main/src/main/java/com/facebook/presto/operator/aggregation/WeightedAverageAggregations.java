/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.DoubleAndDoubleState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("weighted_avg")
public final class WeightedAverageAggregations
{
    private WeightedAverageAggregations() {}

    @InputFunction
    public static void input(
            @AggregationState DoubleAndDoubleState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.DOUBLE) double weight
    )
    {
        state.setFirst(state.getFirst() + weight*value);
        state.setSecond(state.getSecond() + weight);
    }

    @CombineFunction
    public static void combine(@AggregationState DoubleAndDoubleState state, @AggregationState DoubleAndDoubleState otherState)
    {
        state.setFirst(state.getFirst() + otherState.getFirst());
        state.setSecond(state.getSecond() + otherState.getSecond());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState DoubleAndDoubleState state, BlockBuilder out)
    {
        double denominator = state.getSecond();
        if (denominator == 0.0d) {
            out.appendNull();
        }
        else {
            double numerator = state.getFirst();
            DOUBLE.writeDouble(out, numerator / denominator);
        }
    }
}
