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

import com.google.common.base.Preconditions;


/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-10-29
 * Time: 上午11:33
 */
public class ReflectionUtils {
    private static ClassLoader classLoader=null;
    static {
        classLoader=Thread.currentThread().getContextClassLoader();
        if(null == classLoader){
            classLoader=ReflectionUtils.class.getClassLoader();
        }
    }
    public static Object newInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Preconditions.checkNotNull(null!=className,"class name null");
        return classLoader.loadClass(className).newInstance();
    }

    public static Class getClass(String className) throws ClassNotFoundException {
         return classLoader.loadClass(className);
    }

}
