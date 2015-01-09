package tv.icntv.data.change;/*
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.data.change.mr.CmsDataMapper;
import tv.icntv.data.change.mr.CmsDataRecuder;
import tv.icntv.recommend.common.MapReduceUtils;
import tv.icntv.recommend.core.db.DataProcessAbstractJob;
import tv.icntv.recommend.domain.FilmMsg;

import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/07
 * Time: 09:46
 */
public class HiveToHbase extends DataProcessAbstractJob {
    private static final String from = "hive.hdfs.source.directory";
    private static final String hbaseTo = "hbase.target.table";

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        Job hive2Hbase = new Job(configuration, "cms数据导入hbase");
        MapReduceUtils.initMapperJob(CmsDataMapper.class, Text.class, FilmMsg.class, HiveToHbase.class, hive2Hbase, new Path(strings[0]));
        TableMapReduceUtil.initTableReducerJob(strings[1], CmsDataRecuder.class, hive2Hbase);

        return hive2Hbase.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("hive2Hbase.xml");
        String[] array = {configuration.get(from), configuration.get(hbaseTo)};
        ToolRunner.run(configuration,new HiveToHbase(),array);
    }
}
