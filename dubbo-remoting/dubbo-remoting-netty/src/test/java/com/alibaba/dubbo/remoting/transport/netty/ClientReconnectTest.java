/*
 * Copyright 1999-2011 Alibaba Group.
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
package com.alibaba.dubbo.remoting.transport.netty;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.DubboAppender;
import com.alibaba.dubbo.common.utils.LogUtil;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Client;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Server;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.Exchangers;
import com.alibaba.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * 客户端重连测试
 * @author chao.liuc
 *
 */
public class ClientReconnectTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientReconnectTest.class);

    @After
    public void tearDown() throws Exception {
        DubboAppender.clear();
    }

    @Test
    public void testReconnect() throws RemotingException, InterruptedException{
        {
            int port = NetUtils.getAvailablePort();
            Client client = startClient(port, 200);
            Assert.assertEquals(false, client.isConnected());
            Server server = startServer(port);

            while (!client.isConnected()) {
                Thread.sleep(10);
                LOGGER.info("client not reconnected.........................................");
            }
            Assert.assertEquals(true, client.isConnected());
            client.close(2000);
            server.close(2000);
        }
        {
            int port = NetUtils.getAvailablePort();
            Client client = startClient(port, 20000);
            Assert.assertEquals(false, client.isConnected());
            Server server = startServer(port);
            for(int i=0;i<5;i++){
                Thread.sleep(200);
            }
            Assert.assertEquals(false, client.isConnected());
            client.close(2000);
            server.close(2000);
        }
    }
    
    /**
     * 重连日志的校验，时间不够shutdown time时，不能有error日志，但必须有一条warn日志
     */
    @Test
    public void testReconnectWarnLog() throws RemotingException, InterruptedException{
        int port = NetUtils.getAvailablePort();
        DubboAppender.doStart();
        String url = "exchange://127.0.0.2:"+port + "/client.reconnect.test?check=false&"
        +Constants.RECONNECT_KEY+"="+1 ; //1ms reconnect,保证有足够频率的重连
        try{
            ExchangeClient client = Exchangers.connect(url);
            //时间不够长，不会产生error日志
            Assert.assertEquals("no error message ", 0 , LogUtil.findMessage(Level.ERROR, "client reconnect to "));
            client.reconnect();
            //第一次重连失败就会有warn日志
            Assert.assertTrue("must have one warn message ", LogUtil.findMessage(Level.WARN, "client reconnect to ") >= 1);
        }catch (Exception e) {
            //do nothing
        }finally {
            DubboAppender.doStop();
        }
    }
  
    /**
     * 重连日志的校验，不能一直抛出error日志.
     */
    @Test
    public void testReconnectErrorLog() throws RemotingException, InterruptedException{
        int port = NetUtils.getAvailablePort();
        DubboAppender.doStart();
        String url = "exchange://127.0.0.3:"+port + "/client.reconnect.test?check=false&"
        +Constants.RECONNECT_KEY+"="+1 + //1ms reconnect,保证有足够频率的重连
        "&"+Constants.SHUTDOWN_TIMEOUT_KEY+ "=1";//shutdown时间足够短，确保error日志输出
        try{
            Exchangers.connect(url);
        }catch (Exception e) {
            //do nothing
        }
        Thread.sleep(1500);//重连线程的运行
        Assert.assertEquals("only one error message ", 1 , LogUtil.findMessage(Level.ERROR, "client reconnect to "));
        DubboAppender.doStop();
    }
    
    /**
     * 测试client重连方法不会导致重连线程失效.
     */
    @Test
    public void testClientReconnectMethod() throws RemotingException, InterruptedException{
        int port = NetUtils.getAvailablePort();
        String url = "exchange://127.0.0.3:"+port + "/client.reconnect.test?check=false&"
        +Constants.RECONNECT_KEY+"="+10 //1ms reconnect,保证有足够频率的重连
        +"&reconnect.waring.period=1";
        DubboAppender.doStart();
        Client client = Exchangers.connect(url);
        try {
			client.reconnect();
		} catch (Exception e) {
			//do nothing
		}
        Thread.sleep(1500);//重连线程的运行
        Assert.assertTrue("have more then one warn msgs . bug was :" + LogUtil.findMessage(Level.WARN, "client reconnect to "),LogUtil.findMessage(Level.WARN, "client reconnect to ") >1);
        DubboAppender.doStop();
    }
    
    /**
     * 重连日志的校验
     */
    @Test
    public void testReconnectWaringLog() throws RemotingException, InterruptedException{
        int port = NetUtils.getAvailablePort();
        DubboAppender.doStart();
        String url = "exchange://127.0.0.4:"+port + "/client.reconnect.test?check=false&"
        +Constants.RECONNECT_KEY+"="+1 //1ms reconnect,保证有足够频率的重连
        +"&"+Constants.SHUTDOWN_TIMEOUT_KEY+ "=1"//shutdown时间足够短，确保error日志输出
        +"&reconnect.waring.period=100";//每隔多少warning记录一次
        try{
            Exchangers.connect(url);
        }catch (Exception e) {
            //do nothing
        }
        int count =  0;
        for (int i=0;i<100;i++){
            count =  LogUtil.findMessage(Level.WARN, "client reconnect to ") ; 
            if (count >=1){
                break;
            }
            Thread.sleep(50);//重连线程的运行
        }
        Assert.assertTrue("warning message count must >= 1, real :"+count, count>= 1);
        DubboAppender.doStop();
    }
    
    public Client startClient(int port , int reconnectPeriod) throws RemotingException{
        final String url = "exchange://127.0.0.1:"+port + "/client.reconnect.test?check=false&"+Constants.RECONNECT_KEY+"="+reconnectPeriod;
        return Exchangers.connect(url);
    }
    
    public Server startServer(int port) throws RemotingException{
        final String url = "exchange://127.0.0.1:"+port +"/client.reconnect.test";
        return Exchangers.bind(url, new HandlerAdapter());
    }
    
    static class HandlerAdapter extends ExchangeHandlerAdapter{
        public void connected(Channel channel) throws RemotingException {
        }
        public void disconnected(Channel channel) throws RemotingException {
        }
        public void caught(Channel channel, Throwable exception) throws RemotingException {
        }
    }
}