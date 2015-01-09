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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import tv.icntv.recommend.algorithm.correlate.*;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.AbstractJob;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-2
 * Time: 下午4:47
 */
public class CorrelateJob extends AbstractJob {

    private static String sourceProperty="hdfs.directory.input";
    private static String minSupportProperty="icntv.algorithm.correlate.minSupport";
    private static String fpGrowthProperty="hdf.directory.correlate.growth.temp";
    private static String correlateInputProperty="hdf.directory.correlate.growth.input";
    private static String targetResultProperty="hdfs.directory.correlate.output";
    private static String split="--";

    /**
     * new String[]{
     String.format(configuration.get(sourceProperty),date),
     middleDirectory,
     sb.toString(),
     String.format(configuration.get(targetResultProperty),date)
     }
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration=getConf();
        Date date = getDateAdd(-1);

        String middleDirectory=String.format(configuration.get(correlateInputProperty),date);
        StringBuilder sb = new StringBuilder();
        sb.append("minSupport=").append(configuration.get(minSupportProperty,"3")).append(split)
                .append("maxHeapSize=1024").append(split)
                .append("splitterPattern='[\t ]'").append(split)
                .append("input=").append(middleDirectory).append(split)
                .append("output=").append(String.format(configuration.get(fpGrowthProperty),date));

        HadoopUtils.deleteIfExist(middleDirectory);
        Job correlate = new Job(configuration, "关联推荐算法fp-growth");
        MapReduceUtils.initMapperJob(UserHistoryMapper.class, Text.class, Text.class, this.getClass(), correlate, getInput(configuration,-1));//new Path(String.format(configuration.get(sourceProperty),date))
//        MapReduceUtils.initReducerJob(new Path(middleDirectory), UserHistoryReducer.class, correlate);
        correlate.setReducerClass(UserHistoryReducer.class);
        correlate.setOutputKeyClass(NullWritable.class);
        correlate.setOutputValueClass(Text.class);
//        correlate.setCombinerClass(UserHistoryCombiner.class);
        FileOutputFormat.setOutputPath(correlate,new Path(middleDirectory));
        if(!correlate.waitForCompletion(true)){
            return 1;
        };
        Parameters parameter =getParameter(sb.toString());
        HadoopUtils.deleteIfExist(parameter.get("output"));
        PFPGrowth.runPFPGrowth(parameter, configuration);
        String output= parameter.get("output")+"/frequentpatterns";
        long count= HadoopUtils.count(new Path(output), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part-r-\\d*");
            }
        });
        if(count==0){
            return 1;
        }
        String resultPath=String.format(configuration.get(targetResultProperty),date);
        configuration.setLong("icntv.correlate.total.size",count);
        HadoopUtils.deleteIfExist(resultPath);
        Job result = new Job(configuration,"关联算法结果计算");
        MapReduceUtils.initMapperJob(CorrelateInputMapper.class, Text.class, Text.class, this.getClass(), result, new Path(output));
        result.setInputFormatClass(SequenceFileInputFormat.class);

        MapReduceUtils.initReducerJob(new Path(resultPath),CorrelateOutPutReducer.class,result);
        result.waitForCompletion(true);
        return 0;
    }

    private Parameters getParameter(String strings) {
        Parameters parameters=new Parameters();
        String[]values=strings.split(split);
        for(String v:values){
            String[] kvs=v.split("=");
            if(null == kvs || kvs.length!=2){
                continue;
            }
            parameters.set(kvs[0],kvs[1]);
        }
        return parameters;
    }



    public static void main(String[]args) throws Exception {
        final Configuration configuration= new Configuration();
        configuration.addResource("icntv-correlate-algorithm.xml");
//        Date date = getDateAdd(-1);
//        String middleDirectory=String.format(configuration.get(correlateInputProperty),date);
//        StringBuilder sb = new StringBuilder();
//        sb.append("minSupport=").append(configuration.get(minSupportProperty,"3")).append(split)
//                .append("maxHeapSize=512").append(split)
//                .append("splitterPattern='[\t ]'").append(split)
//                .append("input=").append(middleDirectory).append(split)
//                .append("output=").append(String.format(configuration.get(fpGrowthProperty),date));
        ToolRunner.run(configuration,new CorrelateJob(),args);
    }
}
