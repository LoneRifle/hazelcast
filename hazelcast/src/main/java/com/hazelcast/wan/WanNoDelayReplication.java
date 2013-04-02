/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan;

import com.hazelcast.cluster.AuthorizationOperation;
import com.hazelcast.core.EntryView;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Connection;
import com.hazelcast.spi.ConnectionManager;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.AddressUtil.AddressHolder;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

public class WanNoDelayReplication implements Runnable, WanReplicationEndpoint {

    private Node node;
    private ILogger logger;
    private String groupName;
    private String password;
    private final LinkedBlockingQueue<String> addressQueue = new LinkedBlockingQueue<String>();
    private final LinkedList<RecordUpdate> failureQ = new LinkedList<RecordUpdate>();
    private final BlockingQueue<RecordUpdate> q = new ArrayBlockingQueue<RecordUpdate>(100000);
    private volatile boolean running = true;

    public void init(Node node, String groupName, String password, String... targets) {
        this.node = node;
        this.logger = node.getLogger(WanNoDelayReplication.class.getName());
        this.groupName = groupName;
        this.password = password;
        addressQueue.addAll(Arrays.asList(targets));
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    public void recordUpdated(Record entry) {
//        System.out.println("record updated:"+node.nodeEngine.toObject(record.getKey()));
//        if (!q.offer(ru)) {
//            q.poll();
//            q.offer(ru);
//        }
    }

    public void shutdown() {
        running = false;
    }

    public void run() {
        Connection conn = null;
        while (running) {
            try {
                RecordUpdate ru = (failureQ.size() > 0) ? failureQ.removeFirst() : q.take();
                if (conn == null) {
                    conn = getConnection();
                    if (conn != null) {
                        boolean authorized = checkAuthorization(groupName, password, conn.getEndPoint());
                        if (!authorized) {
                            conn.close();
                            conn = null;
                            if (logger != null) {
                                logger.log(Level.SEVERE, "Invalid groupName or groupPassword! ");
                            }
                        }
                    }
                }
                if (conn != null && conn.live()) {
                    ((TcpIpConnection) conn).getWriteHandler().enqueueSocketWritable(ru.toNewPacket());
                } else {
                    failureQ.addFirst(ru);
                    conn = null;
                }
            } catch (InterruptedException e) {
                running = false;
            } catch (Throwable e) {
                if (logger != null) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
                conn = null;
            }
        }
    }

    @SuppressWarnings("BusyWait")
    Connection getConnection() throws InterruptedException {
        final int defaultPort = node.getConfig().getNetworkConfig().getPort();
        while (running) {
            String targetStr = addressQueue.take();
            try {
                final AddressHolder addressHolder = AddressUtil.getAddressHolder(targetStr, defaultPort);
                final Address target = new Address(addressHolder.address, addressHolder.port);
                final ConnectionManager connectionManager = node.getConnectionManager();
                Connection conn = connectionManager.getOrConnect(target);
                for (int i = 0; i < 10; i++) {
                    if (conn == null) {
                        Thread.sleep(1000);
                    } else {
                        return conn;
                    }
                    conn = connectionManager.getConnection(target);
                }
            } catch (Throwable e) {
                Thread.sleep(1000);
            } finally {
                addressQueue.offer(targetStr);
            }
        }
        return null;
    }

    public boolean checkAuthorization(String groupName, String groupPassword, Address target) {
        Operation authorizationCall = new AuthorizationOperation(groupName, groupPassword);
        Future<Boolean> future = node.nodeEngine.getOperationService().createInvocationBuilder(WanReplicationService.SERVICE_NAME,
                authorizationCall, target).setTryCount(1).build().invoke();
        try {
            return future.get();
        } catch (Exception ignored) {
        }
        return false;
    }

    class RecordUpdate {
        final EntryView<Data,Data> entry;
        final String name;

        RecordUpdate(EntryView entry, String name) {
            this.entry = entry;
            this.name = name;
        }

        public Packet toNewPacket() {
            Packet packet = new Packet(null);
//            packet.name = name;
//            packet.operation = ClusterOperation.CONCURRENT_MAP_ASYNC_MERGE;
//            packet.setKey(dataRecordEntry.getKeyData());
//            packet.setValue(toData(dataRecordEntry));
            return packet;
        }
    }
}
