/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.MultiMapLockParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.parameters.MultiMapMessageType#MULTIMAP_LOCK}
 */
public class MultiMapLockMessageTask extends AbstractPartitionMessageTask<MultiMapLockParameters> {

    public MultiMapLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        DefaultObjectNamespace namespace = new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, parameters.name);
        return new LockOperation(namespace, parameters.key, parameters.threadId, parameters.ttl, -1);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VoidResultParameters.encode();
    }

    @Override
    protected MultiMapLockParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapLockParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "lock";
    }

    @Override
    public Object[] getParameters() {
        if (parameters.ttl == -1) {
            return new Object[]{parameters.key, parameters.ttl, TimeUnit.MILLISECONDS};
        }
        return new Object[]{parameters.key};
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}