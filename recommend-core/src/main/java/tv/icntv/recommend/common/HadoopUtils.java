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

package tv.icntv.recommend.common;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-5
 * Time: 上午9:04
 */
public class HadoopUtils {
    private static Configuration configuration = new Configuration();
    public static boolean isExist(Path path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            return fileSystem.exists(path);
        } catch ( Exception e){
            return false;
        }finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }

    public static void mkdirIfNotExist(Path path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            if(!fileSystem.exists(path)){
                fileSystem.mkdirs(path);
            };
        } catch ( Exception e){
            return ;
        }finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }

    public static void deleteIfExist(String path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            Path p = new Path(path);
            if(fileSystem.exists(p)){
                fileSystem.delete(p,true);
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }

    }
    public static boolean isExist(Path ...paths) throws IOException {
        if(null == paths || paths.length==0){
            return false;
        }
        for(Path path : paths){
            if(!isExist(path)){
                return false;
            }
        }
        return true;
    }

    public static long count(Path path,PathFilter filter) throws Exception {
        FileSystem fileSystem=null;

        try{
            fileSystem=FileSystem.get(configuration);
           FileStatus[] fs =fileSystem.listStatus(path,filter);
            long count=0;
            for(FileStatus f : fs){
                SequenceFile.Reader frequentPatternsReader = new SequenceFile.Reader(fileSystem,
                        f.getPath(), configuration);
                Text key = new Text();
                while (frequentPatternsReader.next(key)){
                    count++;
                }
               frequentPatternsReader.close();
            }
            return count;
        }catch (Exception e){
            throw e;
        }finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }



    public static void main(String[]args) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            FileStatus[] list=fileSystem.listStatus(new Path("/user/hadoop/chinacache/"));
            for(FileStatus status:list){
                System.out.println(status.getPath()+"\t");
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
}
