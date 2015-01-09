package tv.icntv.data.process;/*
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

import org.apache.hadoop.io.Text;
import tv.icntv.recommend.core.db.DataTransformReducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by leixw
 * <p/>  key= icntvId+url
 *      value=watch film time \t generate flow
 * Author: leixw
 * Date: 2014/03/04
 * Time: 10:27
 */
public class DataTimeReducer extends DataTransformReducer<Text,Text,Text,Text> {
    @Override
    protected void processReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long time=0;
        long flow=0;
        for (Iterator<Text> it = values.iterator();it.hasNext();) {
            Text iteratorValue=it.next();
            String[] valuesReduce=iteratorValue.toString().split("\t");
            if(null == valuesReduce|| valuesReduce.length!=2){
                continue;
            }
            time+=Long.parseLong(valuesReduce[0]);
            flow+=Long.parseLong(valuesReduce[1]);
        }
        context.write(key,new Text(time+"\t"+flow));
        context.progress();
    }

    @Override
    public boolean validation(Text key, Iterable<Text> values) {
        return null != key && null != values;
    }
}
