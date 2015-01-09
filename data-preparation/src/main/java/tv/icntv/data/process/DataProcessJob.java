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

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.db.DataProcessAbstractJob;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/04
 * Time: 09:56
 */
public class DataProcessJob extends DataProcessAbstractJob {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public Path[] getPaths(Configuration configuration) throws IOException {
        FileSystem fileSystem=FileSystem.get(configuration);
        List<FileStatus> files= Lists.newArrayList();
        FileStatus[] statuses=fileSystem.listStatus(new Path("/icntv/parser/chinacache/2014-03-07"));
        if(null == statuses){
            System.out.println("............");
        }else{
            System.out.println(statuses.length);
        }
        files.addAll(Arrays.asList(statuses));
        System.out.println(files.size());
        List<Path> paths = Lists.transform(files,new Function<FileStatus, Path>() {
            @Override
            public Path apply( org.apache.hadoop.fs.FileStatus input) {
                return input.getPath();  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        return paths.toArray(new Path[paths.size()]);
    }

    /**
     * @param strings-- 0-->输入目录用,分割 1-->临时输出;2--最终输出，作为算法的输入
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = super.getConf();
        Job timeAndFlow = new Job(configuration, "计算观看时间与流量");
        MapReduceUtils.initMapperJob(DataTimeMapper.class, Text.class, Text.class, DataProcessJob.class, timeAndFlow, getPaths(configuration));
        MapReduceUtils.initReducerJob(new Path(strings[1]), DataTimeReducer.class, timeAndFlow);
        if (!timeAndFlow.waitForCompletion(true)) {
            logger.info("time mapreduce error");
            return 1;
        }

        Job cmsMsgUnit = new Job(configuration, "查找cdn日志对应cms信息并处理");
        MapReduceUtils.initMapperJob(DefaultHbaseMapper.class, Text.class, Text.class, DataProcessJob.class, cmsMsgUnit, new Path(strings[1]));
        FileOutputFormat.setOutputPath(cmsMsgUnit, new Path(strings[2]));
        cmsMsgUnit.setNumReduceTasks(0);
        if (!cmsMsgUnit.waitForCompletion(true)) {
            logger.info("cms msg unit error");
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        configuration.addResource("data-process.xml");
        //输入目录前缀
        String inPrefix = configuration.get("hdfs.data.input.prefix");
        HadoopUtils.mkdirIfNotExist(new Path(inPrefix));
        //目录文件夹
        String directory = configuration.get("hdfs.data.input.directories");
        HadoopUtils.mkdirIfNotExist(new Path(directory));
        //临时目录
        String tmpDirectory = configuration.get("hdfs.temp.data.output.directory");
        HadoopUtils.mkdirIfNotExist(new Path(tmpDirectory));
        //最终输出目录
        String resultDirectory = configuration.get("hdfs.data.output.directory");
        HadoopUtils.mkdirIfNotExist(new Path(resultDirectory));
        //文件产生规则 默认%tF
        String fileGengerateRegular = configuration.get("hdfs.data.file.generate.regular", "%tF");

        HadoopUtils.mkdirIfNotExist(new Path(tmpDirectory));
        String[] arrays = new String[]{directory,
                String.format(tmpDirectory + File.separator + fileGengerateRegular, new Date()),
                String.format(resultDirectory + File.separator + fileGengerateRegular, new Date())};
        int i = ToolRunner.run(configuration, new DataProcessJob(), arrays);
        System.exit(i);
    }
}
