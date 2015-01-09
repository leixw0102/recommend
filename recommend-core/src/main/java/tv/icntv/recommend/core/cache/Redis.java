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

package tv.icntv.recommend.core.cache;

import com.google.common.primitives.Ints;
import redis.clients.jedis.*;
import tv.icntv.recommend.common.LoadProperties;

import java.net.URI;
import java.util.Properties;

/**
* Created with IntelliJ IDEA.
* User: xiaowu lei
* Date: 13-11-15
* Time: 下午1:32
*/
public class Redis {
    Properties propertiesUtils= LoadProperties.loadProperties("redis.properties");
    private  static Redis redis=new Redis();
    public synchronized static Redis getRedisInstance(){
        return redis;
    }
    private static final String REDIS_IP="redis.ip";
    private static final String REDIS_PORT="redis.port";
    private static final String REDIS_PWD="redis.pwd";
    private static final String REDIS_MAXACTIVE="redis.maxactive";
    private static final String REDIS_MAXWAIT="redis.maxwait";
    private static final String REDIS_DB="redis.db";
    private  JedisPool jedisPool;
    private  synchronized void init(){
        if(null == jedisPool){
            JedisPoolConfig config=new JedisPoolConfig();
            config.setMaxActive(Ints.tryParse(propertiesUtils.getProperty(REDIS_MAXACTIVE)));
            config.setMaxWait(Ints.tryParse(propertiesUtils.getProperty(REDIS_MAXWAIT)));
            config.setTestOnBorrow(true);
            jedisPool=new JedisPool(config,
                    propertiesUtils.getProperty(REDIS_IP),
                    Ints.tryParse(propertiesUtils.getProperty(REDIS_PORT)),
                    Protocol.DEFAULT_TIMEOUT,
                    propertiesUtils.getProperty(REDIS_PWD));
        }
    }

    private   Jedis getJedis(){
        init();
        Jedis jedis= jedisPool.getResource();
        jedis.select(Ints.tryParse(propertiesUtils.getProperty(REDIS_DB)));
        return jedis;
    }

    private   void returnResource(Jedis jedis){
        if(null != jedis){
            jedisPool.returnResource(jedis);
        }
    }

    public   <T> T execute(IRedisCache<T> cache) throws Exception{
        Jedis jedis=null;
        try{
            jedis= getJedis();
            return cache.callBack(jedis);
        }catch (Exception e){
           throw e;
        }finally {
           returnResource(jedis);
        }
    }

    public static void main(String[]args) throws Exception {
       URI uri= URI.create("hdfs:") ;
        uri.getPath();
        new Redis().execute(new IRedisCache<Object>() {
            @Override
            public Object callBack(Jedis jedis) throws CacheExecption {
//                jedis.set

                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }
}
