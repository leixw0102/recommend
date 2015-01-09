package tv.icntv.data.stb.process.parser;/*
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

import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/07/22
 * Time: 10:28
 * 日志：104（EPGDETAIL）中progatherId为节目集ID，进入详细页。
 *       106（EPGHISTORY）中EPGID为节目集ID.
 *           （EPGSEARCH）中keyword关键字搜索－－需要通过HTTP接口转换成节目集ID
 *           缺少付费信息
 */
public class UserHobbyMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    /**
     * input program series name ;output program series ids
     */
    private String httpUrl="";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        httpUrl=context.getConfiguration().get("","");
        //super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    /**
     * execute search of user
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          if(null == value || Strings.isEmpty(value.toString())){
              return;
          }
          String v=value.toString();
        String[] values= v.split("|");
        if(values[7].equals("21")){
            //called http url ;
            return ;
        }
        context.write(NullWritable.get(),value);
    }
}
