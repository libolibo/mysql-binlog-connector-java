/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * MySQL二进制日志 V4格式 头部信息处理器
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class EventHeaderV4Deserializer implements EventHeaderDeserializer<EventHeaderV4> {

    /** 事件类型集合*/
    private static final EventType[] EVENT_TYPES = EventType.values();

    /**
     * 解析头部信息
     * @param inputStream 输入流
     * @return            V4格式 头部包装类
     * @throws IOException
     */
    @Override
    public EventHeaderV4 deserialize(ByteArrayInputStream inputStream) throws IOException {
        EventHeaderV4 header = new EventHeaderV4();
        header.setTimestamp(inputStream.readLong(4) * 1000L);
        header.setEventType(getEventType(inputStream.readInteger(1)));
        header.setServerId(inputStream.readLong(4));
        header.setEventLength(inputStream.readLong(4));
        header.setNextPosition(inputStream.readLong(4));
        header.setFlags(inputStream.readInteger(2));
        return header;
    }

    /**
     * 获取对应的事件类型信息
     * @param ordinal 在集合中的位置
     * @return        事件类型信息
     * @throws IOException
     */
    private EventType getEventType(int ordinal) throws IOException {
        if (ordinal >= EVENT_TYPES.length) {
            throw new IOException("未知的事件类型, ordinal:" + ordinal);
        }
        return EVENT_TYPES[ordinal];
    }

}
