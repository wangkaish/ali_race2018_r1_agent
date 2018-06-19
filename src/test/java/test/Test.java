/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.generallycloud.baseio.common.Encoding;
import com.generallycloud.baseio.log.Logger;
import com.generallycloud.baseio.log.LoggerFactory;


/**
 * @author wangkai
 *
 */
public class Test {
    
    private static Logger logger = LoggerFactory.getLogger(Test.class);
    
    public static void writeObject(Object obj, OutputStream writer) throws IOException {
        SerializeWriter out = new SerializeWriter();
        JSONSerializer serializer = new JSONSerializer(out);
        serializer.config(SerializerFeature.WriteEnumUsingToString, true);
        serializer.write(obj);
        out.writeTo(writer,Encoding.UTF8);
        out.close(); // for reuse SerializeWriter buf
        writer.flush();
    }

    public static void writeBytes(byte[] b, PrintWriter writer) {
        writer.print(new String(b));
        writer.flush();
    }
    
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws IOException {
        
    }
    
}
