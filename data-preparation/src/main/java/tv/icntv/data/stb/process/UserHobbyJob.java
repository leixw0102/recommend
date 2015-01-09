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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import tv.icntv.data.stb.process.parser.UserHobbyMapper;
import tv.icntv.recommend.core.db.DataProcessAbstractJob;
import org.apache.hadoop.io.Text;
/**
 * Created by leixw
 * <p/> next version;
 * Author: leixw
 * Date: 2014/07/22
 * Time: 10:17
 * 输入格式:
 * 1.		cntvid
 2.		panel_group_id
 3.		sourceObjectType
 4.		sourceObjectId
 5.		targetObjectType
 6.		targetObjectId
 7.		Keyword
 8.		OperType
 9.		opTime
 10.		DataSource
 11.		Fsource
 12.		EPGCode
 13.		RemoteControl
 输出格式：icntv \t 收藏 \t查找 \t 浏览 \t 购买
                    20%   20%      5%      50%
 */
public class UserHobbyJob extends DataProcessAbstractJob {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=super.getConf();
        Job userHobbyJob = Job.getInstance(conf,"用户喜好处理");

        userHobbyJob.setJarByClass(this.getClass());
        userHobbyJob.setMapperClass(UserHobbyMapper.class);
        userHobbyJob.setMapOutputKeyClass(Text.class);
        userHobbyJob.setMapOutputValueClass(NullWritable.class);
        userHobbyJob.setNumReduceTasks(0);
        return userHobbyJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[]args){

    }
}
