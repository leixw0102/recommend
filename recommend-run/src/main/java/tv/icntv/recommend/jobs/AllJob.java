package tv.icntv.recommend.jobs;/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.data.stb.process.StbProcessJob;
import tv.icntv.recommend.algorithm.CFRecommendJob;
import tv.icntv.recommend.algorithm.CorrelateJob;
import tv.icntv.recommend.algorithm.TopNJob;
import tv.icntv.recommend.common.HadoopUtils;
import tv.icntv.recommend.common.ReplaceUtils;
import tv.icntv.recommend.core.AbstractJob;

import java.util.Date;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/09
 * Time: 11:16
 */
public class AllJob extends AbstractJob{
    protected Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
//        //数据init
//        if(!runTool(StbProcessJob.class,args)){
//            logger.error("data  StbProcessJob init error");
//            return 1;
//        }
        //topN
        if(!runTool(TopNJob.class,args)){
            logger.error("data TopNJob init error");
            return 1;
        }
        //cf
        if(!runTool(CFRecommendJob.class,args)){
            logger.error("data CFRecommendJob init error");
            return 1;
        }
        //correlate
        if(!runTool(CorrelateJob.class,args)){
            logger.error("data CorrelateJob init error");
            return 1;
        }
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.addResource("stb-data-process.xml");
        configuration.addResource("icntv-topN-algorithm.xml");
        configuration.addResource("icntv-correlate-algorithm.xml");
        configuration.addResource("icntv-cf-algorithm.xml");
//        // source input
//        String input =configuration.get("hdfs.data.input");
//        // unit cms output directory
//        String stbCmsUnitOutput=configuration.get("hdfs.data.output.directory");
//        HadoopUtils.deleteIfExist(stbCmsUnitOutput);
//
//        //top n
//
//        Date date = getDateAdd(-1);
//        new String[]{
//                ReplaceUtils.formatAll(input,date),
//                String.format(stbCmsUnitOutput, date),
//        }
        int i = ToolRunner.run(configuration,new AllJob(),args);
        System.exit(i);
    }
}
