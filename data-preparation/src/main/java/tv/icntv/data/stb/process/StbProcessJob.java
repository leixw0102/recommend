package tv.icntv.data.stb.process;/*
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.data.stb.process.db.DefaultStbHbaseMapper;
import tv.icntv.recommend.common.DateUtils;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.db.DataProcessAbstractJob;

import java.io.IOException;

/**
 * Created by leixw
 * STB 智能推荐数据整合
 * <p/>
 * Author: leixw
 * Date: 2014/06/30
 * Time: 16:17
 * describe:
 */
public class StbProcessJob extends DataProcessAbstractJob {
    private String beforeMK = "hdfs.data.before.month";
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public int run(String[] args) throws Exception {


        Configuration configuration = super.getConf();

//        Path[] inputs = getInput(configuration);
//        Integer beforeMonth = configuration.getInt(beforeMK, -3);
//        DateTime start = DateUtils.getPlusMonth(beforeMonth);
        DateTime end = DateUtils.getDay(-1);
//        List<DateTime> days = DateUtils.getDay(start, end);
        final String inputFormat = configuration.get("hdfs.data.input");
//        List<Path> inputs = Lists.newArrayList();
//        for (DateTime day : days) {
            String path1 = String.format(inputFormat, end.toDate());
            logger.info("format={},path={}", inputFormat, path1);
            try {
                if (HadoopUtils.isExist(new Path(path1))) {
                    Job cmsMsgUnit = Job.getInstance(configuration, "剔除 contentView 无用信息 "+path1);
                    MapReduceUtils.initMapperJob(DefaultStbHbaseMapper.class, NullWritable.class, Text.class, StbProcessJob.class, cmsMsgUnit,new Path(path1) );  //getInput(configuration)
                    //最终输出目录
                    String resultDirectory = configuration.get("hdfs.data.output.directory");
                    String output = String.format(resultDirectory, end.toDate()); //getDateAdd(-1)
                    HadoopUtils.deleteIfExist(output);

                    FileOutputFormat.setOutputPath(cmsMsgUnit, new Path(output));
                    cmsMsgUnit.setNumReduceTasks(0);
                    cmsMsgUnit.waitForCompletion(true);
                }
            } catch (IOException e) {
            }
        return 0;
    }



    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.addResource("stb-data-process.xml");


        int i = ToolRunner.run(configuration, new StbProcessJob(), args);
        System.exit(i);
    }
}
