package tv.icntv.data.change.mr;/*
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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Text;
import redis.clients.jedis.Jedis;
import tv.icntv.recommend.core.cache.CacheExecption;
import tv.icntv.recommend.core.cache.IRedisCache;
import tv.icntv.recommend.core.cache.Redis;
import tv.icntv.recommend.domain.FilmMsg;

import java.io.IOException;
import java.net.URL;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/07
 * Time: 10:03
 */
public class CmsDataRecuder extends TableReducer<Text, FilmMsg, ImmutableBytesWritable> {

    Redis redis = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        redis = Redis.getRedisInstance();
    }

    @Override
    protected void reduce(Text key, Iterable<FilmMsg> values, Context context) throws IOException, InterruptedException {
        if (null == key || null == values) {
            return;
        }
        final String tempKey = key.toString();
//        final URL url;
//        try {
//            url = new URL(tempKey);
//        } catch (Exception e) {
//            return;
//        }
        final String value = JSON.toJSONString(Lists.newArrayList(values));
        Put put = new Put(Bytes.toBytes(tempKey));
        put.add(Bytes.toBytes("base"), Bytes.toBytes("msg"), Bytes.toBytes(value));
//        Put put1 = new Put(Bytes.toBytes(url.getPath()));
//        put1.add(Bytes.toBytes("base"), Bytes.toBytes("msg"), Bytes.toBytes(value));
        try {
            redis.execute(new IRedisCache<Object>() {
                @Override
                public Object callBack(Jedis jedis) throws CacheExecption {
                    jedis.set(tempKey, value);
//                    jedis.set(url.getPath(), value);
                    return null;
                }
            });
        } catch (Exception e) {

        }
        context.write(new ImmutableBytesWritable(put.getRow()), put);
//        context.write(new ImmutableBytesWritable(put1.getRow()), put1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        redis = null;
    }
}
