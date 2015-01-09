package tv.icntv.data.change.mr;/*
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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import tv.icntv.recommend.core.db.DataTransformMapper;
import tv.icntv.recommend.domain.FilmMsg;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/07
 * Time: 10:02
 */
public class CmsDataMapper extends DataTransformMapper<LongWritable,Text,Text,FilmMsg>{
    @Override
    protected void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        List<String> values= Lists.newArrayList(Splitter.on("\t").trimResults().split(text.toString()));
        if(null == values || values.size()!=12){
            return;
        }
        String url = values.get(1);
        URL u=null;
        try{
            u=new URL(url);
        }catch (Exception e){
            return;
        }
        long time=0L;
        try {
            time=Long.parseLong(values.get(2));
        }catch (Exception e){
            return;
        }


        FilmMsg filmMsg=null;
        try{
            filmMsg =new FilmMsg.FileMsgBuilder().setId(Long.parseLong(values.get(0))).setUrl(url).setTime(time)
                .setName(values.get(3)).setZone(values.get(4)).setYear(values.get(5)).setDirector(values.get(6)).setActors(values.get(7))
                .setTag(values.get(8)).setCategory(values.get(9)).setWriter(values.get(10)).setProgramId(Long.parseLong(values.get(11))).setPpvCode("").builder();
        }catch (Exception e){
            e.printStackTrace();
            return;
        }
        context.write(new Text(values.get(0)),filmMsg);
    }

    @Override
    public boolean validation(LongWritable longWritable, Text text) {
        return null !=text;
    }
}
