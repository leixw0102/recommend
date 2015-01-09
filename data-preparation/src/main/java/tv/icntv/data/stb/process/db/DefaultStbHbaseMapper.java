/* Copyright 2013 Future TV, Inc.
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

package tv.icntv.data.stb.process.db;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.data.process.DBLoadThread;
import tv.icntv.data.process.dbunit.HbaseLoad;
import tv.icntv.recommend.core.db.DataTransformMapper;
import tv.icntv.recommend.domain.FilmMsg;
import tv.icntv.recommend.domain.Films;

import java.io.IOException;
import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: xiaowu lei
* Date: 13-11-21
* Time: 下午5:16
* icntvId|time|    id   |	url		 |openTimeCost    |bufferCount             |buffAverTimeCost        |seekCount           |seekTimeAverTimeCost       |playTotalTimeCost  |error
 |时间|播放标示	|播放地址|视频加载时长(ms)|视频卡的次数，即缓冲次数|视频卡时平均加载时长(ms)|视频快进快退次数(ms)|视频快进快退平均加载时长(ms)|视频播放总时长(ms)|播放器抛出的error信息值，若无误，则值为no
*/

public class DefaultStbHbaseMapper extends DataTransformMapper<LongWritable, Text, NullWritable, Text> {

     private Logger logger = LoggerFactory.getLogger(getClass());
     private String table=null;
     DBLoadThread loadThread=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration=context.getConfiguration();
        table=configuration.get("hbase.table.film.base.msg","icntv.grade.base.msg");
        loadThread=DBLoadThread.getInstance();
    }

    @Override
    protected void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(Splitter.on("|").limit(11).split(text.toString()));
        String programSeriesId = values.get(8);
        String programId = values.get(9);
        if(Strings.isEmpty(programId) || Strings.isEmpty(programSeriesId)){
            return;
        }
//
//        if(!values.get(10).equalsIgnoreCase("no")){
//            return;
//        }


//        String url =values.get(3);
        List<FilmMsg> filmMsg =loadThread.execute(new HbaseLoad(new String[]{programId,table},programId));

        if (null == filmMsg||filmMsg.isEmpty()) {
            logger.info("find msg by programId {}, key {} null,table {}", programId, values.get(0),table);
            return;
        }
        // 流量0，兼容cdn
//        context.write(new Text(values.get(0)+url),new Text(values.get(9)+"\t"+"0\t"+ Films.toString(filmMsg)));
        context.write(NullWritable.get(),new Text(values.get(0)+"\t" + values.get(1)+"\t"+values.get(2)+"\t"+programSeriesId+"\t"+programId+"\t"+filmMsg.get(0).getTime()*60));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        loadThread=null;
    }

    @Override
    public boolean validation(LongWritable longWritable, Text text) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
