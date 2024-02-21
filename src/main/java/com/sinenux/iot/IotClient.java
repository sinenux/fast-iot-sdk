/*
 * MIT License
 *
 * Copyright (c) 2024 sinenux
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.sinenux.iot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.sinenux.iot.util.StringUtil;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * IotClient
 *
 * @author cgm
 */
public class IotClient implements MqttCallback, MqttCallbackExtended {

    private static final Logger log = LoggerFactory.getLogger(IotClient.class);

    private MqttAsyncClient mqttClient = null;
    private MessageListener messageListener = null;
    private String instanceId = null;
    private String consumerToken = null;

    private long lastHeartbeat;
    private long heartBeatInterval = 3 * 1000;

    private int retriedLimit = 3;

    public void init(String serverURI, String instanceId, String consumerToken, MessageListener messageListener, long interval) throws Exception {
        if (messageListener == null) {
            log.error("messageListener 没有指定");
            throw new RuntimeException("messageListener 没有指定");
        }

        MemoryPersistence persistence = new MemoryPersistence();
        MqttConnectOptions connectionOptions = new MqttConnectOptions();
        connectionOptions.setCleanSession(true);
        if (StringUtil.isNotBlank(consumerToken)) {
            connectionOptions.setPassword(consumerToken.toCharArray());
        }
        if (StringUtil.isNotBlank(instanceId)) {
            connectionOptions.setUserName(instanceId);
        }

        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String clientId = "Consumer@" + instanceId + "_" + consumerToken + "_" + uuid;

        mqttClient = new MqttAsyncClient(serverURI, clientId, persistence);

        this.mqttClient.setCallback(this);
        this.messageListener = messageListener;
        this.instanceId = instanceId;
        this.consumerToken = consumerToken;

        if (interval < heartBeatInterval) {
            this.heartBeatInterval = 3 * 1000;
        } else {
            this.heartBeatInterval = interval;
        }

        int count = 0;
        while (!mqttClient.isConnected()) {
            try {
                long startTime = System.currentTimeMillis();
                if (startTime - lastHeartbeat > heartBeatInterval) {
                    lastHeartbeat = startTime;
                    log.info("IOT ================> 请求连接");
                    mqttClient.connect(connectionOptions);
                    count += 1;
                }

                if (mqttClient.isConnected()) {
                    break;
                }

                if (count >= retriedLimit) {
                    log.info("IOT ================> 连接失败");
                    break;
                }
            } catch (MqttException me) {
                int reasonCode = me.getReasonCode();
                log.error("ERROR: " + reasonCode);
            }
        }
    }

    /**
     * Called when the connection to the server is completed successfully.
     *
     * @param reconnect If true, the connection was the result of automatic reconnect.
     * @param serverURI The server URI that the connection was made to.
     */
    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("IOT ================> 连接成功");

        try {
            mqttClient.subscribe("$SYS/" + instanceId + "/" + consumerToken + "/status", 1);
            mqttClient.subscribe("$SYS/" + instanceId + "/" + consumerToken + "/update", 1);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * This method is called when the connection to the server is lost.
     *
     * @param cause the reason behind the loss of connection.
     */
    @Override
    public void connectionLost(Throwable cause) {
        // 连接丢失后，一般在这里面进行重连
        log.info("IOT ================> 连接断开: {}", cause.getMessage());

        // 重连
        while (!mqttClient.isConnected()) {
            log.info("IOT ================> 尝试重新连接");
            try {
                // 这个是3秒后重连
                TimeUnit.MILLISECONDS.sleep(heartBeatInterval);
                mqttClient.reconnect();
            } catch (Exception e) {
                continue;
            }

            if (mqttClient.isConnected()) {
                break;
            }
        }
    }

    /**
     * This method is called when a message arrives from the server.
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        this.messageListener.onMessage(new String(mqttMessage.getPayload()));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Leave it blank for subscriber
    }

    public void publishMessage(String productKey, String deviceSn, String topic, String message, int qos) {
        if (mqttClient.isConnected()) {
            try {
                String sysTopic = "$SYS/" + instanceId + "/" + consumerToken + "/publish";

                JSONObject obj = JSON.parseObject(message);
                JSONObject contentObj = new JSONObject(true);
                contentObj.put("topic", topic);
                contentObj.put("qos", qos);
                contentObj.put("payload", obj);

                JSONObject jsonObject = new JSONObject(true);
                jsonObject.put("instanceId", instanceId);
                jsonObject.put("consumerToken", consumerToken);
                jsonObject.put("productKey", productKey);
                jsonObject.put("deviceSn", deviceSn);
                jsonObject.put("content", contentObj);
                jsonObject.put("generateTime", System.currentTimeMillis());

                byte[] payload = jsonObject.toJSONString().getBytes();
                MqttMessage mqttmessage = new MqttMessage(payload);
                mqttmessage.setQos(0);

                mqttClient.publish(sysTopic, mqttmessage);

                log.debug("推送地址:{}", mqttClient.getCurrentServerURI());
                log.debug("***********************************************************************");
                log.info("IOT ======= 发送 =======> 消息主题 : " + topic);
                log.info("IOT ======= 发送 =======> 消息内容 :\n" + message);
                log.debug("***********************************************************************");
            } catch (MqttException me) {
                log.error("ERROR", me);
            } catch (JSONException e) {
                log.error("ERROR: message can not cast to JSONObject");
            }
        } else {
            log.error("IOT ================> 已断开连接，无法发送消息");
        }
    }

    public void destroy() {
        try {
            if (this.mqttClient != null) {
                this.mqttClient.disconnect().waitForCompletion();
                this.mqttClient.close();
                this.mqttClient = null;
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }

    }

}
