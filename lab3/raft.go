// Package raft contains the Lab 3 Raft peer skeleton.
// raft 包包含 Lab 3 的 Raft 节点代码框架。
//
// Implement the TODOs in stages: 3A (elections), 3B (log replication),
// 3C (persistence), and 3D (snapshots).
// 请分阶段实现 TODO：3A（选举）、3B（日志复制）、3C（持久化）和 3D（快照）。
package raft

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1 "
)

type role int

const (
	follower role = iota
	candidate
	leader
)

// logEntry is one command in the replicated log. TODO(3B): decide how the
// logEntry 表示复制日志中的一条命令。TODO(3B)：确定
// first index is represented when a snapshot has compacted the log prefix.
// 快照压缩日志前缀后，如何表示第一条日志的索引。
type logEntry struct {
	Command interface{}
	Term    int
}

// Raft is one peer in a fixed-size Raft group.
// Raft 表示固定规模 Raft 集群中的一个节点。
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester1.Persister
	me        int
	applyCh   chan raftapi.ApplyMsg

	// Volatile election state (Figure 2). TODO(3A): initialize and protect it.
	// 易失性选举状态（图 2）。TODO(3A)：初始化并保护这些状态。
	currentTerm int
	votedFor    int
	state       role
	//lastHeard   int64 // TODO(3A): replace with the timeout representation you choose.
	// TODO(3A)：替换为你选择的超时表示方式。
	heartBeatCh chan int // 容量 1；收到有效心跳/投票授权时非阻塞发送，用于重置选举计时器

	dead int32
	
	// Volatile log replication state. TODO(3B): initialize and maintain these.
	// 易失性日志复制状态。TODO(3B)：初始化并维护这些状态。
	log         []logEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// Snapshot bookkeeping. TODO(3D): define the index/term meaning consistently.
	// 快照相关记录。TODO(3D)：统一索引和任期的含义。
	snapshotIndex int
	snapshotTerm  int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// TODO(3B): add conflict optimization fields if you use them.
	// TODO(3B)：如果采用冲突优化，请添加相应字段。
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// GetState returns the peer's current term and its belief about leadership.
// GetState 返回该节点当前的任期，以及它对自身是否为领导者的判断。
func (rf *Raft) GetState() (int, bool) {
	// TODO(3A): read term and role under rf.mu.
	// TODO(3A)：在 rf.mu 保护下读取任期和角色。
	rf.mu.lock()
	defer rf.mu.Unlock()

	return rf.currentTerm,rf.state ==leader
}

// Start begins agreement on command and returns immediately.
// Start 开始就 command 达成一致的过程，并立即返回。
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// TODO(3B): append only if leader, then trigger replication.
	// TODO(3B)：仅当本节点是领导者时追加命令，然后触发日志复制。
	return -1, -1, false
}

// RequestVote handles a candidate's election request.
// RequestVote 处理候选者发来的选举请求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO(3A): implement term updates, log freshness, vote granting,
	// persistence, and election-timeout reset.
	// TODO(3A)：实现任期更新、日志新旧判断、投票授权、状态持久化以及选举超时重置。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted=false
	reply.Term=rf.currentTerm

	    // 1) 候选人任期过旧，直接拒绝
		if args.Term<rf.currentTeam
		{
			return;

		}
		// 2) 看到更高任期：更新并转为 follower
		if args.Term>rf.currentTerm
		{
			rf.currentTerm=args.Term
			
			rf.votedFor=-1
			rf.state=follower
			rf.persist()
		}
 // 3) 日志新旧判断：候选人日志必须至少和本地一样新
 	lastIdx :=rf.lastLogIndex()
 	lastTerm :=rf.lastLogTerm()
	upToDate :=args.LastLogTerm>lastTerm ||(args.LastLogTerm==lastTerm && args.LastLogIndex>=lastIdx)
	if !upToDate{
		return
	}

	 // 4) 同一任期只能投一票（重复请求幂等放行）
	 if rf.VotedFor==-1 ||rf.votedFor==args.CandidateId{
		rf.votedFor=args.CandidateId
		rf.persist()
		rf.resetElectionTimer() // 授予投票 -> 重置选举超时

}

// AppendEntries handles heartbeats and log replication from the leader.
// AppendEntries 处理领导者发来的心跳和日志复制请求。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {//3A处理心跳和任期
	// TODO(3A): recognize a valid leader heartbeat.
	// TODO(3A)：识别有效的领导者心跳。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success=false
	reply.Term=rf.currentTerm
	  // 1) 旧任期 leader 的心跳：拒绝   并告知当前任期
	  if args.Term<rf.currentTerm{
		return 
	  }

	  // 2) 有效 leader（任期 >= 本地）：更新任期、转 follower、重置选举计时器
	  if args.Term>rf.currentTerm{
		rf.currentTerm=args.Term
		rf.votedFor=-1
		rf.state=follower
		rf.persist()
	  }
	  rf.state=follower
	  rf.resetElectionTimer() // 收到有效心跳 -> 重置选举超时

	// TODO(3B): check the previous entry, reconcile the log, and advance commitIndex.
	// TODO(3B)：检查前一条日志、协调本地日志，并推进 commitIndex。
	// TODO(3C): persist any state changes before replying.
	// TODO(3C)：回复前持久化所有状态变更。
}

// InstallSnapshot receives a compacted log prefix from the leader.
// InstallSnapshot 接收领导者发送的、经过压缩的日志前缀快照。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// TODO(3D): validate term, update snapshot/log state, persist, then notify applyCh.
	// TODO(3D)：验证任期、更新快照和日志状态、持久化，然后通知 applyCh。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term=rf.currentTerm
	
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[peer].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
}

// electionLoop starts elections after randomized periods without leader contact.
// electionLoop 在一段随机时长内未收到领导者消息后发起选举。
func (rf *Raft) electionLoop() {
	// TODO(3A): run until killed; use randomized election timeouts and start elections.
	// TODO(3A)：持续运行直至节点被终止；使用随机选举超时并发起选举。

	for !rf.killed()
	{
		timeout := time.Duration(300+rand.Int63()%150)*time.Millisecond
		timer := time.NewTimer(timeout)
		select{
		case<-rf.heartBeatCh:
			if ! timer.Stop()
			{

				select{
				case<-time.C:
				default:

				}
			}
		case<-time.C:
		rf.startElection()

		}
	}
}

// heartbeatLoop periodically asks followers to append entries (or a heartbeat).
// heartbeatLoop 定期请求各跟随者追加日志，或发送心跳。
func (rf *Raft) heartbeatLoop() {
	// TODO(3A/3B): replicate to followers while this peer remains leader.
	// TODO(3A/3B)：只要本节点仍是领导者，就持续向跟随者复制日志。
}

// applier sends committed entries to the service in increasing index order.
// applier 按索引递增的顺序将已提交日志项发送给服务。
func (rf *Raft) applier() {
	// TODO(3B): coordinate commitIndex/lastApplied and send ApplyMsg values.
	// TODO(3B)：协调 commitIndex 和 lastApplied，并发送 ApplyMsg 消息。
}

func (rf *Raft) persist() {
	// TODO(3C): encode currentTerm, votedFor, and the log; save with persister.
	// TODO(3C)：编码 currentTerm、votedFor 和日志，并通过 persister 保存。
}

func (rf *Raft) readPersist(data []byte) {
	// TODO(3C): decode the persisted state, handling empty state on first boot.
	// TODO(3C)：解码持久化状态，并处理首次启动时状态为空的情况。
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// TODO(3D): discard compacted log entries and persist state plus snapshot.
	// TODO(3D)：丢弃已压缩的日志项，并持久化状态和快照。
}

func (rf *Raft) Kill() {
	// TODO(3A): mark this peer stopped so its background goroutines can exit.
	// TODO(3A)：标记该节点已停止，以便其后台协程退出。
	rf.killed = true
}

// Make constructs a peer and starts its background goroutines. It must return
// promptly; peers may initially have persisted state from a prior incarnation.
// Make 创建一个节点并启动其后台协程。该函数必须尽快返回；节点启动时可能
// 已包含此前实例留下的持久化状态。
func Make(peers []*labrpc.ClientEnd, me int, persister *tester1.Persister,
	applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,
		votedFor:  -1,
		state:     follower,
		log:       make([]logEntry, 1), // TODO(3B/3D): define the sentinel entry/index convention.
		// TODO(3B/3D)：确定哨兵日志项及其索引的约定。
		commitIndex:   0,
		lastApplied:   0,
		snapshotIndex: 0,
		heartBeatCh: make(chan bool, 1),////？？？？？？？
	}

	// TODO(3C): restore persisted state before starting goroutines.
	// TODO(3C)：启动协程前恢复持久化状态。


	// TODO(3A): launch electionLoop; launch replication/applier loops as needed.
	// TODO(3A)：启动 electionLoop，并按需启动日志复制和应用协程。

	go.rf.electionLoop()
	go.rf.heartbestLoop()


	return rf
}
//辅函数
func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1//len？？？

}
func(rf *Raft) resetElectionTimer()
{
	Select{
	case rf.heartBeatCh<-true:
	default:
	}
}
func(rf* Raft) startElection()
{
	rf.mu.Lock()
	if rf.state == leader {
        rf.mu.Unlock()
        return
    }

	rf.state=candidate
	rf.currentTerm++
	rf.votedFor=rf.me
	rf.persist()//????

	 term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
    lastTerm := rf.log[lastIdx].Term
	rf.mu.Unlock()

	 votes := 1 // 投给自己
    voteCh := make(chan bool, len(rf.peers))

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

	go func(peer int)
		{
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId: rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			
			if !rf.sendRequestVote(p, args, reply) {
                voteCh <- false
                return
            }
            rf.mu.Lock()
            defer rf.mu.Unlock()///////rf.mu??????
			rf.resetElectionTimer()///这里需要吗？？？？
			if reply.Term > rf.currentTerm {
                rf.currentTerm = reply.Term
                rf.votedFor = -1
                rf.state = follower
                rf.persist()
                voteCh <- false
                return
            }

			  // 只处理本轮、本任期、仍是候选人的结果
            if rf.state != candidate || rf.currentTerm != term {
                voteCh <- false
                return
            }
            voteCh <- reply.VoteGranted
		}(peer)
     }
			
    //  for循环开始   ？？？？！！！

	
}