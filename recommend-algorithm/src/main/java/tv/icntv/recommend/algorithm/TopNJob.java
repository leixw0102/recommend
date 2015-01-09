package tv.icntv.recommend.algorithm;/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.recommend.algorithm.topN.ContentCountMapper;
import tv.icntv.recommend.algorithm.topN.ContentCountReducer;
import tv.icntv.recommend.algorithm.topN.NumCountMapper;
import tv.icntv.recommend.algorithm.topN.NumCountReducer;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.AbstractJob;

import java.util.Date;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/11
 * Time: 10:04
 */
public class TopNJob extends AbstractJob {
//    @Override
//    public int run(String[] strings) throws Exception {
//        Configuration configuration = getConf();
////        configuration.setLong("mapred.min.split.size",512*1024*1024L);
//        Job numJob =  Job.getInstance(configuration, "count user cost times");
//        Date date = getDateAdd(-1);
//
//        String input=String.format(configuration.get("hdfs.directory.input"),date );
//        String temp=String.format(configuration.get("hdf.directory.topN.temp"), date);
//        String output=String.format(configuration.get("hdfs.directory.topN.output"), date);
//
//        HadoopUtils.deleteIfExist(temp);
//        MapReduceUtils.initMapperJob(NumCountMapper.class, Text.class, Text.class, this.getClass(), numJob, new Path(input));
//        MapReduceUtils.initReducerJob(new Path(temp), NumCountReducer.class, numJob);
//        numJob.waitForCompletion(true);
//
//        Job programeSets = Job.getInstance(configuration, "计算topN");
//        HadoopUtils.deleteIfExist(output);
//
//        MapReduceUtils.initMapperJob(NumProgramSetsMapper.class, Text.class, Text.class, this.getClass(), programeSets, new Path(temp));
//        programeSets.setCombinerClass(NumProgramSetCombiner.class);
//        MapReduceUtils.initReducerJob(new Path(output), NumProgramSetsReducer.class, programeSets);
//
//        return programeSets.waitForCompletion(true) ? 0 : 1;
//    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.addResource("icntv-topN-algorithm.xml");

        int i = ToolRunner.run(configuration, new TopNJob(), args);
        System.exit(i);
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job numJob =  Job.getInstance(configuration, "count user cost times");
        Date date = getDateAdd(-1);
//        String input=String.format(configuration.get("hdfs.directory.input"),date );
//        String temp=String.format(configuration.get("hdf.directory.topN.temp"), date);
        String output=String.format(configuration.get("hdfs.directory.topN.output"), date);
        HadoopUtils.deleteIfExist(output);
        MapReduceUtils.initMapperJob(ContentCountMapper.class, LongWritable.class, IntWritable.class, this.getClass(), numJob, getInput(configuration));
        numJob.setMapOutputKeyClass(LongWritable.class);
        numJob.setMapOutputValueClass(IntWritable.class);
        MapReduceUtils.initReducerJob(new Path(output), ContentCountReducer.class, numJob);
        numJob.waitForCompletion(true);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
