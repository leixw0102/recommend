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

package tv.icntv.recommend.algorithm.topN;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-22
 * Time: 上午11:57
 * input:
 *      url,map(id,programId).toJson
 * output:
 *      url,id\t programId(`分割) \t num
 */
public class NumCountReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<Text> vs=Lists.newArrayList(values);
        if(null == vs || vs.isEmpty()){
            return;
        }
        Map<Long,Set<Long>>  jsonMaps = JSON.parseObject(vs.get(0).toString(),new TypeReference<Map<Long, Set<Long>>>(){});
        StringBuffer programIds=new StringBuffer();
        StringBuffer programSetIds=new StringBuffer();
        Set<Long> setKeys=jsonMaps.keySet();
        if(null==jsonMaps|| jsonMaps.isEmpty() || null == setKeys || setKeys.isEmpty()){
            return;
        }
        for(Long k:setKeys){
            programIds.append(k).append(",");
            programSetIds.append(processSetsIds(jsonMaps.get(k))).append(",");
        }
        if(Strings.isNullOrEmpty(programIds.toString())|| Strings.isNullOrEmpty(programSetIds.toString())){
            return;
        }
        context.write(key,new Text(programIds.substring(0,programIds.length()-1)+"\t"+programSetIds.substring(0,programSetIds.length()-1)+"\t"+vs.size()));
    }

    private String processSetsIds(Set<Long> values) {
        StringBuffer s = new StringBuffer();
        for(Long v:values){
            s.append(v).append("`");
        }
        return s.substring(0,s.length()-1);
    }
}
