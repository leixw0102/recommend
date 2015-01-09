package tv.icntv.recommend.core.db;/*
 * Copyright 2014 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

import org.apache.hadoop.mapreduce.Reducer;
import tv.icntv.recommend.core.ReduceParameterValidation;

import java.io.IOException;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/03
 * Time: 10:38
 */
public abstract class DataTransformReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements ReduceParameterValidation<KEYIN,VALUEIN> {
    @Override
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
        if(!validation(key, values)){
            return;
        }
        processReduce(key,values,context);
    }

    protected abstract void processReduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException;
}
