/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.book;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private HashMap<String, BigDecimal> map = new HashMap<>();

    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;

    /**
     * Creates a new Worker instance.
     *
     * @param hostPort
     */
    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1,
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Deals with session events like connecting
     * and disconnecting.
     *
     * @param e new event generated
     */
    public void process(WatchedEvent e) {
        LOG.info("worker-" + serverId + " - " + e.toString() + ", " + hostPort);
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    /*
                     * Registered with ZooKeeper
                     */
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("worker-" + serverId + " - Session expired");
                default:
                    break;
            }
        }
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap() {
        createAssignNode();
    }

    void createAssignNode() {
        // create an assignment znode for the master to pass a task to a worker
        zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }

    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem.
                     * If the znode has already been created, then we get a
                     * NODEEXISTS event back.
                     */
                    createAssignNode();
                    break;
                case OK:
                    LOG.info("worker-" + serverId + " - Assign node created");
                    break;
                case NODEEXISTS:
                    LOG.warn("worker-" + serverId + " - Assign node already registered");
                    break;
                default:
                    LOG.error("worker-" + serverId + " - Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register() {
        name = "worker-" + serverId;
        // add worker znode to represent the worker
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem.
                     * If the znode has already been created, then we get a
                     * NODEEXISTS event back.
                     */
                    register();

                    break;
                case OK:
                    LOG.info("worker-" + serverId + " - Registered successfully");

                    break;
                case NODEEXISTS:
                    LOG.warn("worker-" + serverId + " - Already registered");

                    break;
                default:
                    LOG.error("worker-" + serverId + " - Something went wrong: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };

    String status;

    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            // set the data of the status znode to update worker status
            zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    /*
     ***************************************
     ***************************************
     * Methods to wait for new assignments.*
     ***************************************
     ***************************************
     */

    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-" + serverId).equals(e.getPath());

                getTasks();
            }
        }
    };

    void getTasks() {
        // get the children of the worker znode and set watcher to know what tasks it has been assigned
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }


    protected ChildrenCache assignedTasksCache = new ChildrenCache();

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;

                            /*
                             * Initializes input of anonymous class
                             */
                            public Runnable init(List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;

                                return this;
                            }

                            public void run() {
                                if (children == null) {
                                    return;
                                }

                                LOG.info("worker-" + serverId + " - Looping into tasks");
                                setStatus("Working");
                                for (String task : children) {
                                    LOG.trace("New task: {}", task);
                                    // get data from assignment znode to know its command is
                                    zk.getData("/assign/worker-" + serverId + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, final String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    // try to get data again upon connection loss
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;

                        // Initializes the variables this anonymous class needs
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        public void run() {

                            String taskString = new String(data);
                            String[] task = taskString.split("\\s+");
                            String result = "";

                            LOG.info("worker-" + serverId + " - Executing task: " + taskString);

                            if (task[0].equals("insert")) {
                                BigDecimal number = new BigDecimal(task[2]);
                                map.put(task[1], number);
                                result = "The number " + number.toString() + " has been stored under the key '" + task[1] + "'";
                            } else if (task[0].equals("retrieve")) {
                                BigDecimal number = map.get(task[1]);
                                result = "The number associated with '" + task[1] + "' is " + number.toString();
                            } else if (task[0].equals("delete")) {
                                map.remove(task[1]);
                                result = "The number associated with '" + task[1] + "' has been deleted";
                            } else if (task[0].equals("+")) {
                                result = "+";
                                BigDecimal calculation = new BigDecimal(0);
                                for (int i = 1; i < task.length; i++) {
                                    if (map.containsKey(task[i])) {
                                        BigDecimal number = map.get(task[i]);
                                        result += " " + number;
                                        String sub = calculation + " + " + number;
                                        calculation = calculation.add(number);
                                        sub += " = " + calculation;
                                        LOG.info("worker-" + serverId + " - Performing subcalculation: " + sub);
                                    } else {
                                        result += " ?";
                                    }
                                }
                                result += " " + calculation;
                            } else if (task[0].equals("-")) {
                                result = "-";
                                for (int i = 1; i < task.length; i++) {
                                    if (map.containsKey(task[i])) {
                                        BigDecimal number = map.get(task[i]);
                                        result += " " + number;
                                    } else {
                                        result += " ?";
                                    }
                                }
                                if (result.contains("?")) {
                                    result += " " + null;
                                } else {
                                    String[] command = result.split("\\s+");
                                    BigDecimal calculation = new BigDecimal(command[1]);
                                    for (int i = 2; i < command.length; i++) {
                                        String sub = calculation + " - " + command[i];
                                        calculation = calculation.subtract(new BigDecimal(command[i]));
                                        sub += " = " + calculation;
                                        LOG.info("worker-" + serverId + " - Performing subcalculation: " + sub);
                                    }
                                    result += " " + calculation;
                                }
                            } else if (task[0].equals("*")) {
                                result = "*";
                                BigDecimal calculation = null;
                                for (int i = 1; i < task.length; i++) {
                                    if (map.containsKey(task[i])) {
                                        BigDecimal number = map.get(task[i]);
                                        result += " " + number;
                                        if (calculation == null) {
                                            calculation = number;
                                        } else {
                                            String sub = calculation + " * " + number;
                                            calculation = calculation.multiply(number);
                                            sub += " = " + calculation;
                                            LOG.info("worker-" + serverId + " - Performing subcalculation: " + sub);
                                        }
                                    } else {
                                        result += " ?";
                                    }
                                }
                                result += " " + calculation;
                            } else if (task[0].equals("/")) {
                                result = "/";
                                for (int i = 1; i < task.length; i++) {
                                    if (map.containsKey(task[i])) {
                                        BigDecimal number = map.get(task[i]);
                                        result += " " + number;
                                    } else {
                                        result += " ?";
                                    }
                                }
                                if (result.contains("?")) {
                                    result += " " + null;
                                } else {
                                    String[] command = result.split("\\s+");
                                    BigDecimal calculation = new BigDecimal(command[1]);
                                    for (int i = 2; i < command.length; i++) {
                                        String sub = calculation + " / " + command[i];
                                        calculation = calculation.divide(new BigDecimal(command[i]));
                                        sub += " = " + calculation;
                                        LOG.info("worker-" + serverId + " - Performing subcalculation: " + sub);
                                    }
                                    result += " " + calculation;
                                }
                            }

                            LOG.info("worker-" + serverId + " - Result: " + result);

                            if (result.contains("?")) {
                                result = "part " + result;
                            } else {
                                result = "result " + result;
                            }
                            LOG.info("worker-" + serverId + " - Updating status for master");
                            // set data of assignment znode to send result to master
                            zk.setData(path, result.getBytes(), -1, statusUpdateCallback, result);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    LOG.error("worker-" + serverId + " - Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Closes the ZooKeeper session.
     */
    @Override
    public void close() {
        LOG.info("worker-" + serverId + " - Closing");
        try {
            // close ZooKeeper
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("worker-" + serverId + " - ZooKeeper interrupted while closing");
        }
    }

    /**
     * Main method showing the steps to execute a worker.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        Worker w = new Worker(args[0]);
        w.startZK();

        while (!w.isConnected()) {
            Thread.sleep(100);
        }
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();

        /*
         * Getting assigned tasks.
         */
        w.getTasks();

        while (!w.isExpired()) {
            Thread.sleep(1000);
        }

    }

}
