package tv.icntv.recommend.run;/*
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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.util.ProgramDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.recommend.common.LoadProperties;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/03
 * Time: 10:51
 */
public class JobDriver {
    Properties properties=LoadProperties.loadProperties("jobMsg.properties");
    private Logger logger = LoggerFactory.getLogger(getClass());
    public void fillJob(ProgramDriver driver) throws Throwable {
        if(driver==null){
            driver=new ProgramDriver();
        }
        Set<Object> keys=properties.keySet();
        if(null == keys|| keys.isEmpty()){
            return;
        }
        for(Iterator<Object> it = keys.iterator();it.hasNext();){
            String key=it.next().toString();
            String value=properties.getProperty(key);
            if(Strings.isNullOrEmpty(value)){
                continue;
            }
           List<String> values= Lists.newArrayList(Splitter.on(",").split(value));
            if(null == values || values.isEmpty() || values.size()!=3){
                continue;
            }
            logger.info("parameter={},className={},describe={}",values.get(0),values.get(1),values.get(2));
            driver.addClass(values.get(0),Class.forName(values.get(1).trim()),values.get(2));
        }
    }
    public static void main(String []args) throws Throwable {
        ProgramDriver driver=new ProgramDriver();
        JobDriver jobDriver=new JobDriver();
        jobDriver.fillJob(driver);
        driver.driver(args);
        System.exit(0);
    }
}
