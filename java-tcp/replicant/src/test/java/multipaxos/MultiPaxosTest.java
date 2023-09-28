package multipaxos;


import command.Command;
import command.Command.CommandType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import kvstore.MemKVStore;
import log.Instance;
import log.Instance.InstanceState;
import log.Log;
import multipaxos.network.*;
import multipaxos.network.Message.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static multipaxos.MultiPaxos.*;
import static multipaxos.MultiPaxosResultType.*;
import static multipaxos.network.Message.MessageType.ACCEPTRESPONSE;
import static multipaxos.network.Message.MessageType.COMMITRESPONSE;
import static multipaxos.network.Message.MessageType.PREPARERESPONSE;
import static multipaxos.network.ResponseType.OK;
import static multipaxos.network.ResponseType.REJECT;
import static org.junit.jupiter.api.Assertions.*;
import static util.TestUtil.makeConfig;
import static util.TestUtil.makeInstance;


class MultiPaxosTest {

  private static final int kNumPeers = 3;
  protected List<Configuration> configs;
  protected List<Log> logs;
  protected List<MultiPaxos> peers;
  protected List<MemKVStore> stores;
  protected List<ServerSocket> sockets;
  protected List<AtomicBoolean> isServerOn;

  public CommitResponse sendCommit(MultiPaxos p, int targetId,
      long ballot, long lastExecuted, long globalLastExecuted)
      throws IOException, InterruptedException {
    if (!isServerOn.get(targetId).get()) {
      return null;
    }
    CommitRequest commitRequest = new CommitRequest(ballot, lastExecuted,
        globalLastExecuted, p.getId());
    ObjectMapper mapper = new ObjectMapper();
    var request = mapper.writeValueAsString(commitRequest);
    var channelId = p.nextChannelId();
    var responseChan = p.addChannel(channelId);
    p.getPeers().get(targetId).stub.sendAwaitResponse(
        MessageType.COMMITREQUEST, channelId, request);
    var response = responseChan.take();
    var commitResponse = mapper.readValue(response, CommitResponse.class);
    return commitResponse;
  }

  public PrepareResponse sendPrepare(MultiPaxos p, int targetId, long ballot)
      throws IOException, InterruptedException {
    if (!isServerOn.get(targetId).get()) {
      return null;
    }
    PrepareRequest prepareRequest = new PrepareRequest(ballot, targetId);
    ObjectMapper mapper = new ObjectMapper();
    var request = mapper.writeValueAsString(prepareRequest);
    var channelId = p.nextChannelId();
    var responseChan = p.addChannel(channelId);
    p.getPeers().get(targetId).stub.sendAwaitResponse(
        MessageType.PREPAREREQUEST, channelId, request);
    var response = responseChan.take();
    var prepareResponse = mapper.readValue(response, PrepareResponse.class);
    return prepareResponse;
  }

  public AcceptResponse sendAccept(MultiPaxos p, int targetId,
      Instance inst)
      throws IOException, InterruptedException {
    if (!isServerOn.get(targetId).get()) {
      return null;
    }
    AcceptRequest acceptRequest = new AcceptRequest(targetId, inst);
    ObjectMapper mapper = new ObjectMapper();
    var s = mapper.writeValueAsString(acceptRequest.getInstance());
    var request = mapper.writeValueAsString(acceptRequest);
    var channelId = p.nextChannelId();
    var responseChan = p.addChannel(channelId);
    p.getPeers().get(targetId).stub.sendAwaitResponse(
        MessageType.ACCEPTREQUEST, channelId, request);
    var response = responseChan.take();
    var acceptResponse = mapper.readValue(response, AcceptResponse.class);
    return acceptResponse;
  }

  public static long leader(MultiPaxos peer) {
    return extractLeaderId(peer.ballot());
  }

  public static boolean isLeader(MultiPaxos peer) {
    return MultiPaxos.isLeader(peer.ballot(), peer.getId());
  }

  public static boolean isSomeoneElseLeader(MultiPaxos peer) {
    return !isLeader(peer) && leader(peer) < kMaxNumPeers;
  }

  public Long oneLeader() {
    var leader = leader(peers.get(0));
    var numLeaders = 0;

    for (var p : peers) {
      if (isLeader(p)) {
        ++numLeaders;
        if (numLeaders > 1 || p.getId() != leader) {
          return null;
        }
      } else if (leader(p) != leader) {
        return null;
      }
    }
    return leader;
  }

  public void startPeerConnection(long id) {
      isServerOn.get((int) id).set(true);
  }

  public void createTcpSocket(int id) {
    var address = configs.get(id).getPeers().get(id);
    int port = Integer.parseInt(address.substring(address.indexOf(":") + 1));
    try {
      ServerSocket socket = new ServerSocket(port);
      sockets.add(socket);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void startPeerServer(int id, MultiPaxos mp) {
    final ExecutorService thread = Executors.newSingleThreadExecutor();
    thread.submit(() -> {
      try {
        while (true) {
          Socket socket = sockets.get(id).accept();
          BufferedReader reader = new BufferedReader(
              new InputStreamReader(socket.getInputStream()));
          PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
          final ExecutorService handler = Executors.newSingleThreadExecutor();
          handler.submit(() -> {
            try {
              while (true) {
                var msg = reader.readLine();
                handlePeerRequest(mp, writer, msg);
              }
            } catch (IOException ignored){}
          });
        }
      } catch (IOException ignored) {}
    });
  }

  public void handlePeerRequest(MultiPaxos mp, PrintWriter writer, String msg) {
    ObjectMapper mapper = new ObjectMapper();
    Message request = null;
    try {
      request = mapper.readValue(msg, Message.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Message finalRequest = request;
    final ExecutorService thread = Executors.newSingleThreadExecutor();
    thread.submit(() -> {
      var message = finalRequest.getMsg();
      var responseJson = "";
      Message m = null;
      var tcpResponse = "";
      try {
        switch (finalRequest.getType()) {
          case PREPAREREQUEST -> {
            var prepareResponse = new PrepareResponse(REJECT, 0, null);
            if (isServerOn.get((int) mp.getId()).get()) {
              var prepareRequest = mapper.readValue(message,
                  PrepareRequest.class);
              prepareResponse = mp.prepare(prepareRequest);
            } else {
              Thread.sleep(500);
            }
            responseJson = mapper.writeValueAsString(prepareResponse);
            m = new Message(PREPARERESPONSE, finalRequest.getChannelId(),
                responseJson);
          }
          case ACCEPTREQUEST -> {
            var acceptResponse = new AcceptResponse(REJECT, 0);
            if (isServerOn.get((int) mp.getId()).get()) {
              var acceptRequest = mapper.readValue(message,
                  AcceptRequest.class);
              acceptResponse = mp.accept(acceptRequest);
            } else {
              Thread.sleep(500);
            }
            responseJson = mapper.writeValueAsString(acceptResponse);
            m = new Message(ACCEPTRESPONSE, finalRequest.getChannelId(),
                responseJson);
          }
          case COMMITREQUEST -> {
            var commitResponse = new CommitResponse(REJECT, 0, 0);
            if (isServerOn.get((int) mp.getId()).get()) {
              var commitRequest = mapper.readValue(message,
                  CommitRequest.class);
              commitResponse = mp.commit(commitRequest);
            } else {
              Thread.sleep(500);
            }
            responseJson = mapper.writeValueAsString(commitResponse);
            m = new Message(COMMITRESPONSE, finalRequest.getChannelId(),
                responseJson);
          }
        }
        tcpResponse = mapper.writeValueAsString(m);
        writer.write(tcpResponse + "\n");
        writer.flush();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    });
  }

  @BeforeEach
  void setUp() {
    this.configs = new ArrayList<>();
    this.logs = new ArrayList<>();
    this.peers = new ArrayList<>();
    this.stores = new ArrayList<>();
    this.sockets = new ArrayList<>();
    this.isServerOn = new ArrayList<>(kNumPeers);

    for (int i = 0; i < kNumPeers; i++) {
      configs.add(makeConfig(i, kNumPeers));
      stores.add(new MemKVStore());
      logs.add(new Log(stores.get(i)));
      isServerOn.add(new AtomicBoolean(false));
      createTcpSocket(i);
    }

    for (int i = 0; i < kNumPeers; i++) {
      var peer = new MultiPaxos(logs.get(i), configs.get(i));
      peers.add(peer);
      startPeerServer(i, peer);
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    for (var socket : sockets) {
      socket.close();
    }
  }

  @Test
  void constructor() {
    assertEquals(kMaxNumPeers, leader(peers.get(0)));
    assertFalse(isLeader(peers.get(0)));
    assertFalse(isSomeoneElseLeader(peers.get(0)));
  }

  @Test
  void nextBallot() {
    for (int id = 0; id < kNumPeers; ++id) {
      var ballot = id;
      ballot += kRoundIncrement;
      assertEquals(ballot, peers.get(id).nextBallot());
    }
  }

  @Test
  void requestsWithLowerBallotIgnored()
      throws IOException, InterruptedException {
    startPeerConnection(0);

    peers.get(0)
        .becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    peers.get(0)
        .becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());

    var staleBallot = peers.get(1).nextBallot();

    var r1 = sendPrepare(peers.get(1), 0, staleBallot);
    assertEquals(REJECT, r1.getType());
    assertTrue(isLeader(peers.get(0)));

    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(staleBallot, index);
    log.Instance instance1 = new log.Instance();
    ObjectMapper mapper = new ObjectMapper();
    var s = mapper.writeValueAsString(instance1);
    var r2 = sendAccept(peers.get(1), 0, instance);
    assertEquals(REJECT, r2.getType());
    assertTrue(isLeader(peers.get(0)));
    assertNull(logs.get(0).at(index));

    var r3 = sendCommit(peers.get(1), 0, staleBallot, 0, 0);
    assertEquals(REJECT, r3.getType());
    assertTrue(isLeader(peers.get(0)));
  }

  @Test
  void requestsWithHigherBallotChangeLeaderToFollower()
      throws IOException, InterruptedException {
    startPeerConnection(0);
    peers.get(0)
        .becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var r1 = sendPrepare(peers.get(1), 0, peers.get(1).nextBallot());
    assertEquals(OK, r1.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(1)
        .becomeLeader(peers.get(1).nextBallot(), logs.get(0).getLastIndex());
    peers.get(0)
        .becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var index = logs.get(0).advanceLastIndex();
    var instance = makeInstance(peers.get(1).nextBallot(), index);
    var r2 = sendAccept(peers.get(1), 0, instance);
    assertEquals(OK, r2.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));

    peers.get(1)
        .becomeLeader(peers.get(1).nextBallot(), logs.get(1).getLastIndex());
    peers.get(0)
        .becomeLeader(peers.get(0).nextBallot(), logs.get(0).getLastIndex());
    assertTrue(isLeader(peers.get(0)));
    var r3 = sendCommit(peers.get(1), 0, peers.get(1).nextBallot(), 0, 0);
    assertEquals(OK, r3.getType());
    assertFalse(isLeader(peers.get(0)));
    assertEquals(1, leader(peers.get(0)));
  }

  @Test
  void commitCommitsAndTrims() throws IOException, InterruptedException {
    startPeerConnection(0);
    var ballot = peers.get(0).nextBallot();
    var index1 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index1));
    var index2 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index2));
    var index3 = logs.get(0).advanceLastIndex();
    logs.get(0).append(makeInstance(ballot, index3));

    var r1 = sendCommit(peers.get(1), 0, ballot, index2, 0);
    assertEquals(OK, r1.getType());
    assertEquals(0, r1.getLastExecuted());
    assertTrue(logs.get(0).at(index1).isCommitted());
    assertTrue(logs.get(0).at(index2).isCommitted());
    assertTrue(logs.get(0).at(index3).isInProgress());

    logs.get(0).execute();
    logs.get(0).execute();

    var r2 = sendCommit(peers.get(1), 0, ballot, index2, index2);
    assertEquals(index2, r2.getLastExecuted());
    assertEquals(OK, r2.getType());
    assertNull(logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));
    assertTrue(logs.get(0).at(index3).isInProgress());
  }

  @Test
  void prepareRespondsWithCorrectInstances()
      throws IOException, InterruptedException {
    startPeerConnection(0);
    var ballot = peers.get(0).nextBallot();

    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    logs.get(0).append(instance1);

    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);
    logs.get(0).append(instance2);

    var index3 = logs.get(0).advanceLastIndex();
    var instance3 = makeInstance(ballot, index3);
    logs.get(0).append(instance3);

    var r1 = sendPrepare(peers.get(1), 0, ballot);
    assertEquals(OK, r1.getType());
    assertEquals(3, r1.getLogs().size());
    assertEquals(instance1, r1.getLogs().get(0));
    assertEquals(instance2, r1.getLogs().get(1));
    assertEquals(instance3, r1.getLogs().get(2));

    var r2 = sendCommit(peers.get(1), 0, ballot, index2, 0);
    assertEquals(OK, r2.getType());

    logs.get(0).execute();
    logs.get(0).execute();

    ballot = peers.get(0).nextBallot();
    var r3 = sendPrepare(peers.get(1), 0, ballot);
    assertEquals(OK, r3.getType());
    assertEquals(3, r3.getLogs().size());
    assertSame(r3.getLogs().get(0).getState(), InstanceState.kExecuted);
    assertSame(r3.getLogs().get(1).getState(), InstanceState.kExecuted);
    assertEquals(r3.getLogs().get(2).getState(), InstanceState.kInProgress);

    var r4 = sendCommit(peers.get(1), 0, ballot, index2, 2);
    assertEquals(OK, r4.getType());

    ballot = peers.get(0).nextBallot();

    var r5 = sendPrepare(peers.get(1), 0, ballot);
    assertEquals(OK, r5.getType());
    assertEquals(1, r5.getLogs().size());
    assertEquals(instance3, r5.getLogs().get(0));
  }

  @Test
  void acceptAppendsToLog() throws IOException, InterruptedException {
    startPeerConnection(0);
    var ballot = peers.get(0).nextBallot();
    var index1 = logs.get(0).advanceLastIndex();
    var instance1 = makeInstance(ballot, index1);
    var index2 = logs.get(0).advanceLastIndex();
    var instance2 = makeInstance(ballot, index2);

    var r1 = sendAccept(peers.get(1), 0, instance1);
    assertEquals(OK, r1.getType());
    assertEquals(instance1, logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));

    var r2 = sendAccept(peers.get(1), 0, instance2);
    assertEquals(OK, r2.getType());
    assertEquals(instance1, logs.get(0).at(index1));
    assertEquals(instance2, logs.get(0).at(index2));
  }

  @Test
  void prepareResponseWithHigherBallotChangesLeaderToFollower()
      throws IOException, InterruptedException {
    for (var p : peers) {
      startPeerConnection(p.getId());
    }

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    peers.get(1)
        .becomeLeader(peers.get(1).nextBallot(), logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot, logs.get(2).getLastIndex());
    peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot, logs.get(2).getLastIndex());

    var r = sendCommit(peers.get(2), 1, peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).runPreparePhase(peer0Ballot);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));
  }

  @Test
  public void acceptResponseWithHighBallotChangesLeaderToFollower()
      throws IOException, InterruptedException {
    for (var p : peers) {
      startPeerConnection(p.getId());
    }

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    peers.get(1)
        .becomeLeader(peers.get(1).nextBallot(), logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot, logs.get(2).getLastIndex());

    var r1 = sendCommit(peers.get(2), 1, peer2Ballot, 0, 0);
    assertEquals(OK, r1.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    var r2 = peers.get(0).runAcceptPhase(peer0Ballot, 1, new Command(), 0);
    assertEquals(kSomeoneElseLeader, r2.type);
    assertEquals(2, r2.leader);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));
  }

  @Test
  public void commitResponseWithHigherBallotChangesLeaderToFollower()
      throws IOException, InterruptedException {
    for (var p : peers) {
      startPeerConnection(p.getId());
    }

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    peers.get(1)
        .becomeLeader(peers.get(1).nextBallot(), logs.get(1).getLastIndex());
    var peer2Ballot = peers.get(2).nextBallot();
    peers.get(2).becomeLeader(peer2Ballot, logs.get(2).getLastIndex());

    var r = sendCommit(peers.get(2), 1, peer2Ballot, 0, 0);
    assertEquals(OK, r.getType());
    assertFalse(isLeader(peers.get(1)));
    assertEquals(2, leader(peers.get(1)));

    assertTrue(isLeader(peers.get(0)));
    peers.get(0).runCommitPhase(peer0Ballot, 0);
    assertFalse(isLeader(peers.get(0)));
    assertEquals(2, leader(peers.get(0)));
  }

  @Test
  public void runPreparePhase() throws InterruptedException, IOException {
    startPeerConnection(0);

    var peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    var peer1Ballot = peers.get(1).nextBallot();
    peers.get(1).becomeLeader(peer1Ballot, logs.get(1).getLastIndex());

    long index1 = 1;
    var i1 = makeInstance(peer0Ballot, index1, CommandType.Put);

    logs.get(0).append(i1);
    logs.get(1).append(i1);

    long index2 = 2;
    var i2 = makeInstance(peer0Ballot, index2);

    logs.get(1).append(i2);

    long index3 = 3;
    var peer0i3 = makeInstance(peer0Ballot, index3, InstanceState.kCommitted,
        CommandType.Del);
    var peer1i3 = makeInstance(peer1Ballot, index3, InstanceState.kInProgress,
        CommandType.Del);

    logs.get(0).append(peer0i3);
    logs.get(1).append(peer1i3);

    long index4 = 4;
    var peer0i4 = makeInstance(peer0Ballot, index4, InstanceState.kExecuted,
        CommandType.Del);
    var peer1i4 = makeInstance(peer1Ballot, index4, InstanceState.kInProgress,
        CommandType.Del);

    logs.get(0).append(peer0i4);
    logs.get(1).append(peer1i4);

    long index5 = 5;
    peer0Ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(peer0Ballot, logs.get(0).getLastIndex());
    peer1Ballot = peers.get(1).nextBallot();
    peers.get(1).becomeLeader(peer1Ballot, logs.get(1).getLastIndex());

    var peer0i5 = makeInstance(peer0Ballot, index5, InstanceState.kInProgress,
        CommandType.Get);
    var peer1i5 = makeInstance(peer1Ballot, index5, InstanceState.kInProgress,
        CommandType.Put);

    logs.get(0).append(peer0i5);
    logs.get(1).append(peer1i5);

    var ballot = peers.get(0).nextBallot();

    assertNull(peers.get(0).runPreparePhase(ballot));

    startPeerConnection(1);

    Thread.sleep(2000);

    ballot = peers.get(0).nextBallot();

    var res = peers.get(0).runPreparePhase(ballot);
    var log = res.getValue();
    var lastIndex = res.getKey();
    assertEquals(index5, lastIndex);
    assertEquals(i1, log.get(index1));
    assertEquals(i2, log.get(index2));
    assertEquals(peer0i3.getCommand(), log.get(index3).getCommand());
    assertEquals(peer0i4.getCommand(), log.get(index4).getCommand());
    assertEquals(peer1i5, log.get(index5));
  }

  @Test
  void runAcceptPhase() throws InterruptedException {
    startPeerConnection(0);

    var ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(ballot, logs.get(0).getLastIndex());
    var index = logs.get(0).advanceLastIndex();

    var result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kRetry, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).at(index).isInProgress());
    assertNull(logs.get(1).at(index));
    assertNull(logs.get(2).at(index));

    startPeerConnection(1);

    Thread.sleep(2000);

    result = peers.get(0).runAcceptPhase(ballot, index, new Command(), 0);

    assertEquals(kOk, result.type);
    assertNull(result.leader);

    assertTrue(logs.get(0).at(index).isCommitted());
    assertTrue(logs.get(1).at(index).isInProgress());
    assertNull(logs.get(2).at(index));
  }

  @Test
  void runCommitPhase() throws InterruptedException, IOException {
    startPeerConnection(0);
    startPeerConnection(1);

    var ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(ballot, logs.get(0).getLastIndex());

    for (long index = 1; index <= 3; ++index) {
      for (int peer = 0; peer < kNumPeers; ++peer) {
        if (index == 3 && peer == 2) {
          continue;
        }
        logs.get(peer)
            .append(makeInstance(ballot, index, InstanceState.kCommitted));
        logs.get(peer).execute();
      }
    }
    long gle = 0;
    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(0, gle);

    startPeerConnection(2);

    logs.get(2).append(makeInstance(ballot, 3));

    Thread.sleep(2000);

    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(2, gle);

    logs.get(2).execute();

    gle = peers.get(0).runCommitPhase(ballot, gle);
    assertEquals(3, gle);
  }

  @Test
  void replay() {
    startPeerConnection(0);
    startPeerConnection(1);

    var ballot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(ballot, logs.get(0).getLastIndex());

    long index1 = 1;
    var i1 = makeInstance(ballot, index1, InstanceState.kCommitted,
        CommandType.Put);
    long index2 = 2;
    var i2 = makeInstance(ballot, index2, InstanceState.kExecuted,
        CommandType.Get);
    long index3 = 3;
    var i3 = makeInstance(ballot, index3, InstanceState.kInProgress,
        CommandType.Del);

    HashMap<Long, log.Instance> log = new HashMap<>();
    log.put(index1, i1);
    log.put(index2, i2);
    log.put(index3, i3);

    assertNull(logs.get(0).at(index1));
    assertNull(logs.get(0).at(index2));
    assertNull(logs.get(0).at(index3));

    assertNull(logs.get(1).at(index1));
    assertNull(logs.get(1).at(index2));
    assertNull(logs.get(1).at(index3));

    var newBallot = peers.get(0).nextBallot();
    peers.get(0).becomeLeader(newBallot, logs.get(0).getLastIndex());
    peers.get(0).replay(newBallot, log);

    i1.setBallot(newBallot);
    i2.setBallot(newBallot);
    i3.setBallot(newBallot);

    i1.setState(InstanceState.kCommitted);
    i2.setState(InstanceState.kCommitted);
    i3.setState(InstanceState.kCommitted);

    assertEquals(i1, logs.get(0).at(index1));
    assertEquals(i2, logs.get(0).at(index2));
    assertEquals(i3, logs.get(0).at(index3));

    i1.setState(InstanceState.kInProgress);
    i2.setState(InstanceState.kInProgress);
    i3.setState(InstanceState.kInProgress);

    assertEquals(i1, logs.get(1).at(index1));
    assertEquals(i2, logs.get(1).at(index2));
    assertEquals(i3, logs.get(1).at(index3));
  }

  @Test
  void replicate() throws InterruptedException {
    peers.get(0).start();
    startPeerConnection(0);

    var result = peers.get(0).replicate(new Command(), 0);
    assertEquals(kRetry, result.type);
    assertNull(result.leader);

    peers.get(1).start();
    peers.get(2).start();
    startPeerConnection(1);
    startPeerConnection(2);

    long commitInterval = configs.get(0).getCommitInterval();
    var commitInterval3x = 3 * commitInterval;

    Thread.sleep(commitInterval3x);

    var leader = oneLeader();
    assertNotNull(leader);

    result = peers.get(Math.toIntExact(leader)).replicate(new Command(), 0);
    assertEquals(kOk, result.type);
    assertNull(result.leader);

    var nonLeader = (leader + 1) % kNumPeers;

    result = peers.get((int) nonLeader).replicate(new Command(), 0);
    assertEquals(kSomeoneElseLeader, result.type);
    assertEquals(leader, result.leader);

    peers.get(0).stop();
    peers.get(1).stop();
    peers.get(2).stop();
  }
}

