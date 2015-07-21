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

package tv.icntv.recommend.algorithm;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import tv.icntv.recommend.algorithm.cf.*;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.AbstractJob;

import java.util.Date;


/**
* Created with IntelliJ IDEA.
* User: xiaowu lei
* Date: 13-11-26
* Time: 下午4:23
*/
public class CFRecommendJob extends AbstractJob {
    private static String cfInputProperty="hdfs.directory.cf.input";
    private static String cfOutProperty="hdfs.directory.cf.output";
    private static String cfTempPropery="hdfs.directory.cf.temp";

    /**
     * new String[]{
     String.format(configuration.get("hdfs.directory.input"),date),
     baseCfData,
     sb.toString(),
     output ,
     temp
     }
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
//        configuration.setLong("mapred.min.split.size",512*1024*1024L);
        Date date = getDateAdd(-1);

        String baseCfData=String.format(configuration.get(cfInputProperty),date);
        String output=String.format(configuration.get(cfOutProperty),date);
        String temp=String.format(configuration.get(cfTempPropery), date);

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(baseCfData);
        sb.append(" --output ").append(output);
        sb.append(" --numRecommendations ").append(configuration.get("icntv.cf.recommend.num.per.user"));
        sb.append(" --similarityClassname ").append(configuration.get("icntv.cf.recommend.similarityClassname"));
        sb.append(" --tempDir ").append(temp);

        HadoopUtils.deleteIfExist(baseCfData);
        Job timeJob = Job.getInstance(configuration, "获取用户对影片的打分");
//        MapReduceUtils.initMapperJob(TimeMaper.class, Text.class, Text.class, this.getClass(), timeJob, new Path(String.format(configuration.get("hdfs.directory.input"),date)));
//        timeJob.setCombinerClass(TimeCombiner.class);
//        MapReduceUtils.initReducerJob(new Path(baseCfData),TimeReducer.class,timeJob);
        MapReduceUtils.initMapperJob(ViewTimeMapper.class,Text.class,Text.class,this.getClass(),timeJob,getInput(configuration)); //new Path(String.format(configuration.get("hdfs.directory.input"),date))
        MapReduceUtils.initReducerJob(new Path(baseCfData), ViewTimeReducer.class,timeJob);
        timeJob.setNumReduceTasks(8);
        timeJob.waitForCompletion(true);

        HadoopUtils.deleteIfExist(output);
        HadoopUtils.deleteIfExist(temp);
        return ToolRunner.run(configuration,new RecommenderJob(),sb.toString().split(" "));
    }

    public static void main(String[]args) throws Exception {
        final Configuration configuration=new Configuration();
        configuration.addResource("icntv-cf-algorithm.xml");



//        String tables = configuration.get("hbase.cdn.tables");
//
//        if (Strings.isNullOrEmpty(tables)) {
//            return;
//        }
//        List<String> list = Splitter.on(",").splitToList(tables);
//        List<String> results = Lists.transform(list, new Function<String, String>() {
//            @Override
//            public String apply(@Nullable String input) {
//                return String.format(configuration.get("hdfs.directory.base.db"), new Date(), input);
//            }
//        });

        int i=ToolRunner.run(configuration,new CFRecommendJob(),args);
        System.exit(i);
    }

}
