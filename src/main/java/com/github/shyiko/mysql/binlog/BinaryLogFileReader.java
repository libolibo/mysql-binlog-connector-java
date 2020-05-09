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
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * MySQL binary log file reader.
 * 读取MySQL二进制文件
 * 2020-05-07
 */
public class BinaryLogFileReader implements Closeable {

    /** 日志开头的魔法数字, 用于判断是否为MySQL二进制文件*/
    public static final byte[] MAGIC_HEADER = new byte[]{(byte) 0xfe, (byte) 0x62, (byte) 0x69, (byte) 0x6e};

    /** 包装输入流*/
    private final ByteArrayInputStream inputStream;

    /** 事件信息反解析器*/
    private final EventDeserializer eventDeserializer;

    /**
     * 构造方法
     * @param file  文件路径
     * @throws IOException
     */
    public BinaryLogFileReader(File file) throws IOException {
        this(file, new EventDeserializer());
    }

    /**
     * 构造方法
     * @param file              文件路径
     * @param eventDeserializer 事件信息解析器
     * @throws IOException
     */
    public BinaryLogFileReader(File file, EventDeserializer eventDeserializer) throws IOException {
        this(file != null ? new BufferedInputStream(new FileInputStream(file)) : null, eventDeserializer);
    }

    /**
     * 构造方法
     * @param inputStream 输入流
     * @throws IOException
     */
    public BinaryLogFileReader(InputStream inputStream) throws IOException {
        this(inputStream, new EventDeserializer());
    }

    /**
     * 构造方法
     * @param inputStream       输入流
     * @param eventDeserializer 事件信息解析器
     * @throws IOException
     */
    public BinaryLogFileReader(InputStream inputStream, EventDeserializer eventDeserializer) throws IOException {
        if (inputStream == null) {
            throw new IllegalArgumentException("输入流不能为空");
        }
        if (eventDeserializer == null) {
            throw new IllegalArgumentException("事件解析器不能为空");
        }
        this.inputStream = new ByteArrayInputStream(inputStream);
        try {
            byte[] magicHeader = this.inputStream.read(MAGIC_HEADER.length);
            if (!Arrays.equals(magicHeader, MAGIC_HEADER)) {
                throw new IOException("不可用的日志文件");
            }
        } catch (IOException e) {
            try {
                this.inputStream.close();
            } catch (IOException ex) {
                // ignore
            }
            throw e;
        }
        this.eventDeserializer = eventDeserializer;
    }

    /**
     * 获取下一个事件信息
     * @return deserialized event or null in case of end-of-stream
     */
    public Event readEvent() throws IOException {
        return eventDeserializer.nextEvent(inputStream);
    }

    /**
     * 关闭文件流
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        inputStream.close();
    }

}
