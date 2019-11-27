package chord;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import repast.simphony.context.Context;
import repast.simphony.engine.schedule.ScheduledMethod;
import repast.simphony.random.RandomHelper;
import repast.simphony.space.graph.Network;
import repast.simphony.space.graph.RepastEdge;
import repast.simphony.util.ContextUtils;

public class Node {
	public static int M;	// total number of positions = 2^M
	
	private Node predecessor, successor;
	private ArrayList<Node> fingerTable, successors;
	private int id, next, timeouts, failures;
	private boolean failed;
	
	private ReentrantReadWriteLock fingerLock, successorsLock;
	
	private ArrayList<Integer> numberOfKeys, pathLengths;
	private RepastEdge<Object> edge;
	
	public Node(int id) {
		this.id = id;
		this.next = 0;
		fingerLock = new ReentrantReadWriteLock();
		successorsLock = new ReentrantReadWriteLock();
		this.numberOfKeys = new ArrayList<>();
		this.pathLengths = new ArrayList<>();
		this.timeouts = 0;
		this.failures = 0;
		this.failed = false;
		this.edge = null;
	}
	
	public void setPredecessor(Node predecessor) {
		this.predecessor = predecessor;
		this.computeNumberOfKeys();
		// System.out.println(this.id + " pred: " + this.predecessor.id + " succ: " + this.successor.id);
	}
	
	public ArrayList<Node> getSuccessors() {
		this.successorsLock.readLock().lock();
		ArrayList<Node> successorsCopy = new ArrayList<>(this.successors);
		this.successorsLock.readLock().unlock();
		return successorsCopy;
	}
	
	public void setSuccessors(ArrayList<Node> successors) {
		this.successors = successors;
		this.successor = this.successors.get(0);
	}
	
	public void setFingerTable(ArrayList<Node> fingerTable) {
		this.fingerTable = fingerTable;
	}
	
	@ScheduledMethod(start = 1, interval = 1)
	public void fixFinger() {
		if (!this.failed) {
			ArrayList<Integer> toRemove = new ArrayList<>();
			for (int i = 0; i < M; i++) {
				int realIndex = this.getFingerValue(i);
				Pair result = this.findSuccessor(realIndex, 0);
				if (result != null) {
					Node newFinger = result.node;
					this.fingerLock.writeLock().lock();
					if (i < this.fingerTable.size())
						this.fingerTable.set(this.next, newFinger);
					else
						this.fingerTable.add(newFinger);
					this.fingerLock.writeLock().unlock();
					this.next = (this.next + 1) % M;
				}
				else {
					toRemove.add(i);
				}
			}
			this.fingerLock.writeLock().lock();
			for (Integer i: toRemove)
				this.fingerTable.remove(i);
			this.fingerLock.writeLock().unlock();
		}
	}
	
	public int getId() {
		return id;
	}
	/**
	 * Get the real index of the i-th item contained in the finger table
	 * @param index, the index to search [1 to M]
	 * @return the real index in the system
	 */
	private int getFingerValue(int index) {
		return (this.id + (int)Math.pow(2, index)) % (int)Math.pow(2, M);
	}
	
	private Node closestPrecedingNode(int id) {
		int fullClock = (int)Math.pow(2, M);
		if (id < this.id)
			id += fullClock;
		this.fingerLock.readLock().lock();
		for (int i = M - 1; i >= 0; i--) {
			Node f = this.fingerTable.get(i);
			int offset = 0;
			if (f.id < this.id)
				offset = fullClock;
			if (f.id + offset > this.id && f.id + offset < id) {
				this.fingerLock.readLock().unlock();
				return f;
			}
		}
		this.fingerLock.readLock().unlock();
		return this;
	}
	
	public Pair findSuccessor(int id, int startingStep) {
		if (this.failed)
			return null;
		if (this.successor.isFailed()) {
			Context<Object> context = ContextUtils.getContext(this);
			Network<Object> net = (Network<Object>)context.getProjection("chord network");
			if (this.edge != null)
				net.removeEdge(this.edge);
			this.edge = null;
			this.successorsLock.readLock().lock();
			for (Node n: this.successors)
				if (n != null && !n.isFailed()) { 
					this.successor = n;
					this.edge = net.addEdge(this, this.successor);
					break;
				} else {
					this.timeouts++;
				}
			this.successorsLock.readLock().unlock();
			if (this.successor.isFailed()) {
				System.err.println("Fatal: all successors have crashed");
				System.exit(1);
			}
		}
		if (this.id == id)
			return new Pair(this, startingStep);
		if (id > this.id && id <= this.successor.getId())
			return new Pair(this.successor, 1 + startingStep);
		if (id > this.id && this.successor.id < this.id)
			return new Pair(this.successor, 1 + startingStep);
		if (this.successor.id < this.id && id <= this.successor.id)
			return new Pair(this.successor, 1 + startingStep);
		Node x = this.closestPrecedingNode(id);
		if (x.isFailed())
			return null;
		return x.findSuccessor(id, startingStep + 1);
	}
	/**
	 * Create a new chord ring
	 */
	public void create() {
		this.successor = this;
		this.predecessor = null;
	}
	
	public void join(Node n) {
		this.successor = n.findSuccessor(this.id, 0).node;
		this.predecessor = null;
	}
	
	public void notify(Node n) {
		if (this.predecessor == null || (n.getId() > this.predecessor.getId() && n.getId() < this.id)) {
			this.predecessor = n;
			this.computeNumberOfKeys();
		}
	}
	
	@ScheduledMethod(start = 1, interval = 1)
	public void stabilize() {
		if (!this.failed) {
			if (this.successor.isFailed()) {
				Context<Object> context = ContextUtils.getContext(this);
				Network<Object> net = (Network<Object>)context.getProjection("chord network");
				if (this.edge != null)
					net.removeEdge(this.edge);
				this.edge = null;
				this.successorsLock.readLock().lock();
				for (Node n: this.successors)
					if (n != null && !n.isFailed()) { 
						this.successor = n;
						this.edge = net.addEdge(this, this.successor);
						break;
					} else {
						this.timeouts++;
					}
				this.successorsLock.readLock().unlock();
				if (this.successor.isFailed()) {
					System.err.println("Fatal: all successors have crashed");
					System.exit(1);
				}
			}
			Node x = successor.predecessor;
			if (x != null && x.id > this.id && x.id < this.successor.id) {
				ArrayList<Node> successors = new ArrayList<>(x.getSuccessors());
				successors.remove(successors.size() - 1);
				successors.add(0, x);
				this.successorsLock.writeLock().lock();
				this.successors = successors;
				this.successorsLock.writeLock().unlock();
			}
			successor.notify(this);
		}
	}
	@ScheduledMethod(start = 1, interval = 1)
	public void checkPredecessor() {
		if (this.predecessor == null || this.predecessor.isFailed())
			this.predecessor = null;
	}
	
	@ScheduledMethod(start = 1, interval = 1)
	public void searchKey() {
		if (!this.failed) {
			int maxKey = (int)Math.pow(2, M);
			int key = RandomHelper.nextIntFromTo(0, maxKey - 1);
			//System.out.println("Node " + this.id + " searches for " + key);
			Pair n = this.findSuccessor(key, 0);
			if (n == null)
				this.failures++;
			else
				this.pathLengths.add(n.steps);
			//System.out.println("Got " + n.id);
		}
	}
	
	private void computeNumberOfKeys() {
		int pred = this.predecessor.id;
		int me = pred > this.id ? this.id + (int)Math.pow(2, M) : this.id;
		this.numberOfKeys.add(me - pred);
	}
	
	public int minKeys() {
		int min = Integer.MAX_VALUE;
		for (Integer i: this.numberOfKeys)
			if (i < min)
				min = i;
		return min;
	}
	
	public int maxKeys() {
		int max = Integer.MIN_VALUE;
		for (Integer i: this.numberOfKeys)
			if (i > max)
				max = i;
		return max;
	}
	
	public double avgKeys() {
		int sum = 0;
		for (Integer i: this.numberOfKeys)
			sum += i;
		return sum * 1.0 / this.numberOfKeys.size();
	}
	
	public int minPathLength() {
		int min = Integer.MAX_VALUE;
		for (Integer i: this.pathLengths)
			if (i < min)
				min = i;
		return min;
	}
	
	public int maxPathLength() {
		int max = Integer.MIN_VALUE;
		for (Integer i: this.pathLengths)
			if (i > max)
				max = i;
		return max;
	}
	
	public double avgPathLength() {
		int sum = 0;
		for (Integer i: this.pathLengths)
			sum += i;
		return sum * 1.0 / this.pathLengths.size();
	}
	
	public int getTimeouts() {
		return timeouts;
	}
	
	public int getFailures() {
		return failures;
	}
	
	@Override
	public String toString() {
		return "" + this.id;
	}

	public boolean isFailed() {
		return failed;
	}
	
	public void fail() {
		this.failed = true;
	}
}
