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

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;

import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers.
 * <p>
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * <p>
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * <p>
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED.
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 * <p>
 * Because workers may crash, this master also needs to be able to reassign
 * tasks. When it watches for changes in the list of workers, it also
 * receives a notification when a znode representing a worker is gone, so
 * it is able to reassign its tasks.
 * <p>
 * A primary may crash too. In the case a primary crashes, the next primary
 * that takes over the role needs to make sure that it assigns and reassigns
 * tasks that the previous primary hasn't had time to process.
 */
public class Master implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */
    enum MasterStates {
        RUNNING, ELECTED, NOTELECTED
    }

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString(random.nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private HashMap<String, String> map = new HashMap<>();
    private LinkedList<String[]> parts = new LinkedList<>();
    private int partsGoal = 0;

    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;

    /**
     * Creates a new master instance.
     *
     * @param hostPort
     */
    Master(String hostPort) {
        this.hostPort = hostPort;
    }


    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException {
        // close ZooKeeper
        zk.close();
    }

    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session.
     *
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }


    /**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
     */
    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        // create parent znode to keep track of ZooKeeper structure
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again. Note that registering again is not a problem.
                     * If the znode has already been created, then we get a
                     * NODEEXISTS event back.
                     */
                    createParent(path, (byte[]) ctx);

                    break;
                case OK:
                    LOG.info("Parent created");

                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);

                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Check if this client is connected.
     *
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * Check if the ZooKeeper session has expired.
     *
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }

    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */


    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of.
     *
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again.
     *
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    break;
                case OK:
                    state = MasterStates.ELECTED;
                    takeLeadership();

                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();

                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master: ", KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    void masterExists() {
        // check if master exists and set watcher to know if a new master must be elected
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();

                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    runForMaster();
                    LOG.info("It sounds like the previous master is gone, so let's run for master again");

                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeDeleted) {
                assert "/master".equals(e.getPath());

                runForMaster();
            }
        }
    };

    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();

        (new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if (rc == RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed");
                } else {
                    LOG.info("Assigning recovered tasks");
                    getTasks();
                }
            }
        });
    }

    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation.
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the
     * znode has been created. In the case the znode exists, it needs to check
     * which server is the master.
     */

    /**
     * Tries to create a /master lock znode to acquire leadership.
     */
    public void runForMaster() {
        LOG.info("Running for master");
        // creates master znode to set itself as the master
        zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    break;
                case NONODE:
                    runForMaster();

                    break;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExists();
                    }

                    break;
                default:
                    LOG.error("Error when reading data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void checkMaster() {
        // gets data from master znode to check who the master is
        zk.getData("/master", false, masterCheckCallback, null);
    }

    /*
     ****************************************************
     ****************************************************
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */


    /**
     * This method is here for testing purposes.
     *
     * @return size Size of the worker list
     */
    public int getWorkersSize() {
        if (workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }

    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeChildrenChanged) {
                assert "/workers".equals(e.getPath());

                getWorkers();
            }
        }
    };

    void getWorkers() {
        // gets children of workers znode to know which workers are available to assign tasks to
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Successfully got list of workers: "
                            + children.size()
                            + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */

    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("Removing and setting");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker) {
        // gets children of assign znode that belongs to a missing worker so the tasks can be reassigned
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);

                    break;
                case OK:
                    LOG.info("Successfully got list of assignments: "
                            + children.size()
                            + " tasks");

                    /*
                     * Reassign the tasks of the absent worker.
                     */

                    for (String task : children) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;
                default:
                    LOG.error("getChildren failed: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        // gets data of task so that it can be reassigned to a new worker
        zk.getData(path, false, getDataReassignCallback, task);
    }

    /**
     * Context for recreate operation.
     */
    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getDataReassign(path, (String) ctx);

                    break;
                case OK:
                    recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

                    break;
                default:
                    LOG.error("Something went wrong when getting data: ", KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Recreate task znode in /tasks
     *
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        // creates a task znode for a task that never returned a result from the worker it was assigned to
        zk.create("/tasks/" + ctx.task, ctx.data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, ctx);
    }

    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    recreateTask((RecreateTaskCtx) ctx);

                    break;
                case OK:
                    deleteAssignment(((RecreateTaskCtx) ctx).path);

                    break;
                case NODEEXISTS:
                    LOG.info("Node exists already, but if it hasn't been deleted, then it will eventually, so we keep trying: " + path);
                    recreateTask((RecreateTaskCtx) ctx);

                    break;
                default:
                    LOG.error("Something went wrong when recreating task: ", KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path) {
        // delete the assignment znode that was either completed or assigned to a worker that is no longer active
        zk.delete(path, -1, taskDeletionCallback, null);
    }

    VoidCallback taskDeletionCallback = new VoidCallback() {
        public void processResult(int rc, String path, Object rtx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    LOG.info("Successfully deleted task: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */

    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals(e.getPath());

                getTasks();
            }
        }
    };

    void getTasks() {
        // get children of tasks znode so they can be assigned to workers
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();

                    break;
                case OK:
                    List<String> toProcess;
                    if (tasksCache == null) {
                        tasksCache = new ChildrenCache(children);

                        toProcess = children;
                    } else {
                        toProcess = tasksCache.addedAndSet(children);
                    }

                    if (toProcess != null) {
                        assignTasks(toProcess);
                    }

                    break;
                default:
                    LOG.error("getChildren failed: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for (String task : tasks) {
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        // get data from task znode so that it can be passed to a worker in an assignment znode
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);

                    break;
                case OK: // mms
                    String designatedWorker;
                    String taskString = new String(data);
                    String[] task = taskString.split("\\s+");

                    LOG.info("Received task: " + taskString);

                    if (task[0].equals("insert")) {
                        // Choose worker at random
                        List<String> list = workersCache.getList();
                        designatedWorker = list.get(random.nextInt(list.size()));
                        map.put(task[1], designatedWorker);
                        LOG.info("Storing '" + task[1] + "' in " + designatedWorker);
                        String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                        createAssignment(assignmentPath, data);
                    } else if (task[0].equals("retrieve") || task[0].equals("delete")) {
                        designatedWorker = map.get(task[1]);
                        LOG.info("Retrieving '" + task[1] + "' from " + designatedWorker);
                        String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                        createAssignment(assignmentPath, data);
                    } else if (task[0].equals("+") || task[0].equals("-") || task[0].equals("*") || task[0].equals("/")) {
                        HashSet<String> workers = new HashSet<>();
                        for (int i = 1; i < task.length; i++) {
                            workers.add(map.get(task[i]));
                        }
                        String message = "Assigning parts of calculation to ";
                        for (String worker : workers) {
                            message += worker + ", ";
                            String assignmentPath = "/assign/" + worker + "/" + (String) ctx;
                            createAssignment(assignmentPath, data);
                        }
                        if (workers.size() > 1) {
                            partsGoal = workers.size();
                        }
                        message = message.substring(0, message.length() - 2);
                        LOG.info(message);
                    }

                    break;
                default:
                    LOG.error("Error when trying to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void createAssignment(String path, byte[] data) {
        // create assignment znode to send task from master to worker
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
        // get data of assignment znode and set watcher to know when a worker returns the assignment result
        zk.getData(path, assignmentWatcher, null, data);
    }

    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);

                    break;
                case OK:
                    LOG.info("Successfully assigned task: " + name);
                    deleteTask(name.substring(name.lastIndexOf("/") + 1));

                    break;
                case NODEEXISTS:
                    LOG.warn("Task already assigned");

                    break;
                default:
                    LOG.error("Error when trying to assign task: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void deleteTask(String name) {
        // delete the task znode once it has been assigned to a worker
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    VoidCallback taskDeleteCallback = new VoidCallback() {
        public void processResult(int rc, String path, Object ctx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask(path);

                    break;
                case OK:
                    LOG.info("Successfully deleted task: " + path);

                    break;
                case NONODE:
                    LOG.info("Task has been deleted already");

                    break;
                default:
                    LOG.error("Something went wrong here: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StringCallback taskStatusCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    // try to create status znode again upon connection loss
                    zk.create(path + "/status", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOG.info("Successfully created status node: " + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: " + path);
                    break;
                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }

        }
    };

    DataCallback assignmentDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);

                    break;
                case OK: // mms
                    String taskID = path.substring(path.length() - 10);
                    String taskString = new String(data);
                    String[] task = taskString.split("\\s+");

                    if (task[0].equals("result")) {
                        if (task[1].equals("+") || task[1].equals("-") || task[1].equals("*") || task[1].equals("/")) {
                            String operand = task[1];
                            String result = "";
                            for (int i = 2; i < task.length - 1; i++) {
                                result += task[i] + " " + operand + " ";
                            }
                            taskString = result.substring(0, result.length() - 2) + "= " + task[task.length - 1];
                        } else {
                            taskString = taskString.substring(7);
                        }
                        LOG.info("Received result of task-" + taskID + ", sending to client");
                        // create status znode to send result of task from master to client
                        zk.create("/status/task-" + taskID, taskString.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskStatusCreateCallback, ctx);
                        deleteAssignment(path);
                    } else if (task[0].equals("part")) {
                        taskString = taskString.substring(5);
                        String[] partialResult = taskString.split("\\s+");
                        parts.add(partialResult);
                        LOG.info("Received part " + parts.size() + " of " + partsGoal + " for task-" + taskID);
                        deleteAssignment(path);
                        if (parts.size() == partsGoal) {
                            LOG.info("Performing remaining calculations on task-" + taskID);
                            String[] command = parts.get(0);
                            for (int i = 1; i < command.length - 1; i++) {
                                int n = 1;
                                while (command[i].equals("?")) {
                                    command[i] = parts.get(n)[i];
                                    n++;
                                }
                            }
                            String operand = parts.get(0)[0];
                            BigDecimal calculation = new BigDecimal(0);
                            if (operand.equals("+")) {
                                for (String[] part : parts) {
                                    calculation = calculation.add(new BigDecimal(part[part.length - 1]));
                                }
                            } else if (operand.equals("*")) {
                                String[] part = parts.get(0);
                                calculation = new BigDecimal(part[part.length - 1]);
                                for (int i = 1; i < parts.size(); i++) {
                                    part = parts.get(i);
                                    calculation = calculation.multiply(new BigDecimal(part[part.length - 1]));
                                }
                            } else if (operand.equals("-")) {
                                calculation = new BigDecimal(command[1]);
                                for (int i = 2; i < command.length - 1; i++) {
                                    calculation = calculation.subtract(new BigDecimal(command[i]));
                                }
                            } else if (operand.equals("/")) {
                                calculation = new BigDecimal(command[1]);
                                for (int i = 2; i < command.length - 1; i++) {
                                    calculation = calculation.divide(new BigDecimal(command[i]));
                                }
                            }
                            String result = "";
                            for (int i = 1; i < command.length - 1; i++) {
                                result += command[i] + " " + operand + " ";
                            }
                            result = result.substring(0, result.length() - 2) + "= " + calculation;
                            LOG.info("Processed result of task-" + taskID + ", sending to client");
                            // create status znode to send result of task from master to client
                            zk.create("/status/task-" + taskID, result.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, taskStatusCreateCallback, ctx);
                            partsGoal = 0;
                            parts = new LinkedList<>();
                        }
                    }

                    break;
                default:
                    LOG.error("Error when trying to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    Watcher assignmentWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if (e.getType() == EventType.NodeDataChanged) {
                // get data of assignment znode and set watcher to trigger logic to handle the result
                zk.getData(e.getPath(), assignmentWatcher, assignmentDataCallback, null);
            }
        }
    };

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    @Override
    public void close() {
        if (zk != null) {
            try {
                // close ZooKeeper
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing ZooKeeper session: ", e);
            }
        }
    }

    /**
     * Main method providing an example of how to run the master.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        Master m = new Master(args[0]);
        m.startZK();

        while (!m.isConnected()) {
            Thread.sleep(100);
        }
        /*
         * bootstrap() creates some necessary znodes.
         */
        m.bootstrap();

        /*
         * now runs for master.
         */
        m.runForMaster();

        while (!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZK();
    }
}
