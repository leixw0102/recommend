package tv.icntv.recommend.common;/*
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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Strings;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/08
 * Time: 17:49
 */
public class ReplaceUtils {
    private static Pattern pattern= Pattern.compile("\\$\\{(.*)}");

    public static String searchReplaceParameter(String input){
        if(Strings.isEmpty(input)){
            return null;
        }
        Matcher matcher=pattern.matcher(input);
        if(matcher.matches()){
            return matcher.group(1);
        }
        return null;
    }

    public static String searchReplaceResult(String input,Configuration configuration){
        String parameter=searchReplaceParameter(input);
        if(Strings.isEmpty(parameter)){
            return input;
        }
        String pv=configuration.get(parameter);
        return input.replaceFirst("${"+parameter+"}",pv);
    }
    private static Map<String,String> timePV= Maps.newConcurrentMap();
    static {
        timePV.put("%tI", "yyyyMMdd");
        timePV.put("%tF","yyyy-MM-dd");
    }

    public static String format(String format,Object obj){
        if(timePV.containsKey(format)){
            return new DateTime(obj).toString(timePV.get(format));
        }
        return null;
    }

    public static String formatAll(String source, Object obj){
        Iterator<String> it = timePV.keySet().iterator();

        while (it.hasNext()){
            String key=it.next();
            if(source.contains(key)){
                source=source.replace(key,format(key,obj));
            }
        }
        return source;
    }

    public static void main(String[]args){
         System.out.println(formatAll("/icntv/parser/stb/cdnAdapter/result/%tF/cdnloginfo_%tI.dat", new Date()));
    }
}
