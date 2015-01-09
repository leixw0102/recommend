package tv.icntv.data.stb.process;/*
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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/24
 * Time: 09:30
 */
public class StbContentViewJob  extends DataProcessAbstractJob {
    private String beforeMK="hdfs.data.before.month";
    protected Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job cmsMsgUnit = Job.getInstance(configuration, "查找stb日志对应cms信息并处理");
        MapReduceUtils.initMapperJob(DefaultStbHbaseMapper.class, Text.class, Text.class, StbProcessJob.class, cmsMsgUnit, getInput(configuration));
        //最终输出目录
        String resultDirectory = configuration.get("hdfs.data.output.directory");
        String output = String.format(resultDirectory,getDateAdd(-1));
        HadoopUtils.deleteIfExist(output);

        FileOutputFormat.setOutputPath(cmsMsgUnit, new Path(output));
        cmsMsgUnit.setNumReduceTasks(0);
        return cmsMsgUnit.waitForCompletion(true)?0:1;
    }
    public Path[] getInput( Configuration configuration){
        Integer beforeMonth = configuration.getInt(beforeMK,-3);
        DateTime start = DateUtils.getPlusMonth(beforeMonth);
        DateTime end = DateUtils.getDay(-1);
        List<DateTime> days = DateUtils.getDay(start,end);
        final String inputFormat=configuration.get("hdfs.data.input");
        List<Path> inputs = Lists.newArrayList();
        for(DateTime day:days){
            String path1=String.format(inputFormat,day.toDate());
            logger.info("format={},path={}",inputFormat,path1);
            try {
                if(HadoopUtils.isExist(new Path(path1))){
                    inputs.add(new Path(path1));
                }
            } catch (IOException e) {

                continue;
            }
        }
        return inputs.toArray(new Path[inputs.size()]);
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.addResource("stb-data-process.xml");
        int i = ToolRunner.run(configuration, new StbProcessJob(), args);
        System.exit(i);
    }
}
