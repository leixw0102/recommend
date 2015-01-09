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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import tv.icntv.recommend.core.db.DataTransformMapper;

import java.io.IOException;

/**
 * Created by leixw
 * key = icntvId+url
 * value=ip</>url</>status</>startTime</>endTime</>flow</>ua
 * <p/>
 * Author: leixw
 * Date: 2014/03/04
 * Time: 09:59
 */
public class DataTimeMapper extends DataTransformMapper<LongWritable,Text,Text,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values=value.toString().split("\t");
        if(null == values || values.length!=2){
             return ;
        }
        String tempKey=values[0];
        if(tempKey.length()<=15){
            return ;
        }
        String temp=tempKey.substring(0,15);
        if(!temp.matches("\\d*")){
            return ;
        }
        String[] otherValues=values[1].split("</>");
        String startTime=otherValues[3],endTime=otherValues[4],flow=otherValues[5];
        if(!startTime.matches("\\d*") || !endTime.matches("\\d*") || !flow.matches("\\d*")){
            System.out.println(value.toString()+"...");
            return;
        }
        context.progress();
        context.write(new Text(tempKey),new Text((Long.parseLong(endTime) - Long.parseLong(startTime))+"\t"+flow));
    }

    @Override
    public boolean validation(LongWritable key, Text value) {

        return true;
    }
}
