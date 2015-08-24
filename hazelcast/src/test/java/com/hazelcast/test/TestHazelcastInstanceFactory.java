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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class TestHazelcastInstanceFactory {

    private static final AtomicInteger ports = new AtomicInteger(5000);
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final AtomicInteger nodeIndex = new AtomicInteger();

    protected TestNodeRegistry registry;
    protected CopyOnWriteArrayList<Address> addresses;
    private int count;

    public TestHazelcastInstanceFactory(int count) {
        this.count = count;
        if (mockNetwork) {
            this.addresses = new CopyOnWriteArrayList<Address>();
            this.addresses.addAll(createAddresses(ports, count));
            this.registry = new TestNodeRegistry(addresses);
        }
    }

    public TestHazelcastInstanceFactory() {
        this.count = 0;
        this.addresses = new CopyOnWriteArrayList<Address>();
        this.registry = new TestNodeRegistry(addresses);
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            this.addresses = new CopyOnWriteArrayList<Address>();
            this.addresses.addAll(createAddresses(ports, addresses));
            this.registry = new TestNodeRegistry(this.addresses);
        }
    }

    /**
     * Delegates to {@link #newHazelcastInstance(Config) <code>newHazelcastInstance(null)</code>}.
     */
    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(null);
    }

    /**
     * Creates a new test Hazelcast instance.
     * @param config the config to use; use <code>null</code> to get the default config.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        final String instanceName = config != null? config.getInstanceName() : null;
        if (mockNetwork) {
            init(config);
            NodeContext nodeContext = registry.createNodeContext(pickAddress());
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        return HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    private Address pickAddress() {
        int id = nodeIndex.getAndIncrement();
        if (addresses.size() > id) {
            return addresses.get(id);
        }
        Address address = createAddress("127.0.0.1", ports.incrementAndGet());
        addresses.add(address);
        return address;
    }

    public HazelcastInstance[] newInstances() {
        return newInstances(new Config());
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = newHazelcastInstance(config);
        }
        return instances;
    }

    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config, count);
    }

    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        if (mockNetwork) {
            return registry.getAllHazelcastInstances();
        }
        return Hazelcast.getAllHazelcastInstances();
    }

    public void shutdownAll() {
        if (mockNetwork) {
            registry.shutdown();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public void terminateAll() {
        if (mockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    private static List<Address> createAddresses(AtomicInteger ports, int count) {
        List<Address> addresses = new ArrayList<Address>(count);
        for (int i = 0; i < count; i++) {
            addresses.add(createAddress("127.0.0.1", ports.incrementAndGet()));
        }
        return addresses;
    }

    private static List<Address> createAddresses(AtomicInteger ports, String... addressArray) {
        checkElementsNotNull(addressArray);

        final int count = addressArray.length;
        List<Address> addresses = new ArrayList<Address>(count);
        for (int i = 0; i < count; i++) {
            addresses.add(createAddress(addressArray[i], ports.incrementAndGet()));
        }
        return addresses;
    }

    protected static Address createAddress(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static <T> void checkElementsNotNull(T[] array) {
        checkNotNull(array, "Array should not be null");
        for (Object element : array) {
            checkNotNull(element, "Array element should not be null");
        }
    }

    private static Config init(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config.setProperty(GroupProperties.PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "120");
        config.setProperty(GroupProperties.PROP_PARTITION_BACKUP_SYNC_INTERVAL, "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TestHazelcastInstanceFactory{");
        sb.append("addresses=").append(addresses);
        sb.append('}');
        return sb.toString();
    }
}
