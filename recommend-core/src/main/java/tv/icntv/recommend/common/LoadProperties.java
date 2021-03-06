package tv.icntv.recommend.common;/*
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

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/03/03
 * Time: 10:55
 */
public class LoadProperties {
    private static Properties properties = new Properties();
    public static Properties loadProperties(String file) {
        InputStream inputStream=getInputStream(file);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return null;
        }
        return properties;
    }

    private static InputStream getInputStream(String file){
       return  LoadProperties.class.getClassLoader().getResourceAsStream(file);
    }

}
