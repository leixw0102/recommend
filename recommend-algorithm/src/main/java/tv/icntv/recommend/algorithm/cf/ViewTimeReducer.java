package tv.icntv.recommend.algorithm.cf;/*
 * Copyright 2014 Future TV, Inc.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.icntv.tv/licenses/LICENSE-1.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import tv.icntv.recommend.common.DateUtils;
import tv.icntv.recommend.common.MathExtend;

import java.io.IOException;
import java.util.*;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/24
 * Time: 15:22
 */
public class ViewTimeReducer extends Reducer<Text, Text, Text, Text> {
    private static final String scoreKey = "grade.total.score";
    private int scoreValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        scoreValue = configuration.getInt(scoreKey, 10);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String[] keys = key.toString().split("\\|");
        Long defaultTime = Long.parseLong(keys[2]);
        if(defaultTime<=0){
            defaultTime=1L;
        }
//        List<String> ks = Lists.transform(Lists.newArrayList(values), new Function<Text, String>() {
//            @Override
//            public String apply(Text input) {
//                return input.toString();  //To change body of implemented methods use File | Settings | File Templates.
//            }
//        });
        List<String> ks = new ArrayList<String>();
        for(Text text:values){
            ks.add(text.toString());
        }
        if(null == ks || ks.isEmpty()){
            return;
        }
        Collections.sort(ks);
        List<String> starts = Lists.newArrayList();
        for (String k : ks) {
            if (k.endsWith("1")) {
                starts.add(k);
            }
        }
        Long times;
        Long totalTime;
        if(null == starts || starts.isEmpty()||starts.size()==0){
            times=defaultTime;
            totalTime = defaultTime*2;
        }else {
            times = getTime(ks, starts, defaultTime);
            totalTime= defaultTime * starts.size();
        }

        double grade = MathExtend.divide(times, totalTime, 5) * scoreValue;
        context.write(new Text(keys[0]), new Text(keys[1] + "\t" + grade));
    }

    public Long getTime(List<String> ks, List<String> starts, long time) {
        long total = 0L;

        String first = starts.get(0);
        int i = ks.indexOf(first);
        if (i != 0) {
            total += time;
        } else {

            for (int x = 1; x < starts.size() - 1; x++) {
                String from = starts.get(x);
                int fromPos = ks.indexOf(from);
                String nextFrom = starts.get(x + 1);
                int nextFromPos = ks.indexOf(nextFrom);

                if (nextFromPos - fromPos == 0) {
                    continue;
                }

                String end = ks.get(nextFromPos - 1);

                long se = check(from,end,time);
                total += se;

            }
            String last = starts.get(starts.size() - 1);
            int lastPos = ks.indexOf(last);
            if (lastPos == ks.size() - 1) {
                total += time;
            } else {
                String other = ks.get(ks.size() - 1);
                total += check(last, other, time);
            }
        }
        return total;
    }

    public long check(String from, String end, long time) {
        int se = Seconds.secondsBetween(DateUtils.getDateTime(from.split("\\|")[0]), DateUtils.getDateTime(end.split("\\|")[0])).getSeconds();
        return se < time ? se : time;
    }

    public static void main(String[] args) {
//        System.out.println(MathExtend.divide(4,(double)5,5));
        List<String> list = Lists.newArrayList();
        Collections.sort(list);
        System.out.println(list);
    }
}
