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

package tv.icntv.data.process.dbunit;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-6
 * Time: 下午5:46
 */
public class HbaseUtils {
    //    private Configuration configuration = null;
    private static HTablePool hTablePool = null;
    private static int poolSize = 500;
    static {
        hTablePool = new HTablePool(HBaseConfiguration.create(), poolSize);
    }

    private HbaseUtils() {

    }

    public static synchronized   HTableInterface getHtable(String table) {
        HTableInterface hTable = hTablePool.getTable(Bytes.toBytes(table));
//        hTable.setAutoFlush(false);

        return hTable;
    }

    public static synchronized   void release(HTableInterface hTable) {
        if (null != hTable) {
            try {
                hTable.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } finally {
                try {
//                    hTablePool.putTable(hTable);
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public static synchronized void save(List<Put> puts,String table){
           HTableInterface hTable=getHtable(table);
        try {
             hTable.put(puts);
        }   catch (Exception e){

        }   finally {
            release(hTable);
        }
    }

    public static synchronized   <T> T callback(IHbaseCallBack<T> callBack,String key,String table){
           HTableInterface hTable=getHtable(table);
            try {
                return callBack.callback(hTable,key);
            }   catch (Exception e){
            }   finally {
                release(hTable);
            }
          return null;
    }
    public static void main(String[] args) throws IOException {
    }
}
