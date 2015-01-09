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

package tv.icntv.recommend.core;


import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * 查询db/hbase/file 等，通过缓存方式。
 * User: xiaowu lei
 * Date: 13-11-20
 * Time: 下午3:50
 */
public abstract class AbstractDB<T> {
    private String[]cause;
    private String key;
    private Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractDB(String[] cause, String key) {
        this.cause = cause;
        this.key = key;
    }


    public String[] getCause() {
        return cause;
    }

    public void setCause(String[] cause) {
        this.cause = cause;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }


    public List<T> get(){
        Preconditions.checkNotNull(cause);
        Preconditions.checkNotNull(key);

        List<T> t =null;
        try{
                t=getCache(key);
        }catch (Exception e){
            System.out.println("get cache exception-->"+e);
        }
        if(null != t && !t.isEmpty()){
            return t;
        }
        return null;
//        logger.info("read cache redis for key={},result null",key);
//        final List<T> msg= getDB(cause);
//        if(null == msg || msg.isEmpty()){
//            logger.info("read db for key={},result null",key);
//            return null;
//        }
//        Redis.execute(new IRedisCache<Boolean>() {
//            @Override
//            public Boolean callBack(Jedis jedis) throws CacheExecption {
//                jedis.set(key,JSON.toJSONString(msg));
//                return true;
//            }
//        }) ;
//        try {
//            setCache(key,msg);
//        }catch (Exception e){
//            logger.info("save to cache error-->"+e);
//        }

//        return msg;
    }

    public abstract List<T> getCache(String key);

    public abstract List<T> getDB(String[] cause);

    public abstract void setCache(final String key,final List<T> values);
}
