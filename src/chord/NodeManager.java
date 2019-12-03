package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import repast.simphony.context.Context;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.engine.schedule.ScheduledMethod;
import repast.simphony.random.RandomHelper;
import repast.simphony.space.continuous.ContinuousSpace;
import repast.simphony.space.graph.Network;

public class NodeManager {
	private HashMap<Integer, Node> allNodes;
	private double failProb, center, radius;
	private Context<Object> context;
	private ContinuousSpace<Object> space;
	private boolean disasterMode, done;
	private int joinLeave, totalKeys;
	private Network<Object> net;
	
	public NodeManager(Context<Object> context, ContinuousSpace<Object> space, Network<Object> net, HashMap<Integer, Node> allNodes, double failProb, boolean disasterMode, int joinLeave, int totalKeys, double center, double radius) {
		this.allNodes = allNodes;
		this.failProb = failProb;
		this.context = context;
		this.disasterMode = disasterMode;
		this.joinLeave = joinLeave;
		this.done = false;
		this.totalKeys = totalKeys;
		this.center = center;
		this.radius = radius;
		this.space = space;
		this.net = net;
	}
	
	@ScheduledMethod(start = 1, priority = ScheduleParameters.FIRST_PRIORITY, interval = 1)
	public void killNodes() {
		if (this.disasterMode && !this.done) {
			ArrayList<Node> nodes = new ArrayList<>(this.allNodes.values());
			Collections.shuffle(nodes);
			int upTo = (int)(this.failProb * nodes.size());
			upTo = upTo < nodes.size() ? upTo : nodes.size();
			for (int i = 0; i < upTo; i++) {
				nodes.get(i).fail();
				context.remove(nodes.get(i));
				allNodes.remove(nodes.get(i).getId());
			}
			System.out.println(upTo + " nodes have failed");
			this.done = true;
		} else if (!this.disasterMode) {
			ArrayList<Node> nodes = new ArrayList<>(this.allNodes.values());
			Collections.shuffle(nodes);
			ArrayList<Integer> failed = new ArrayList<>();
			for (int i = 0; i < this.joinLeave; i++) {
				nodes.get(i).leave();
				failed.add(nodes.get(i).getId());
				context.remove(nodes.get(i));
				allNodes.remove(nodes.get(i).getId());
			}
			System.out.println("Nodes " + failed + " failed");
			nodes = new ArrayList<>(this.allNodes.values());
			for (int i = 0; i < this.joinLeave; i++) {
				int nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
				while (this.allNodes.containsKey(nextKey) || failed.contains(nextKey))
					nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
				Node randomKnown = nodes.get(RandomHelper.nextIntFromTo(0, nodes.size() - 1));
				Node n = new Node(nextKey, this.net);
				context.add(n);
				if (n.join(randomKnown)) {
					allNodes.put(nextKey, n);
					double theta = 2 * Math.PI * nextKey / totalKeys;
			        double x = center + radius * Math.cos(theta);
			        double y = center + radius * Math.sin(theta);
			        space.moveTo(n, x, y);
			        System.out.println("Node " + nextKey + " started");
				} else {
					context.remove(n);
					System.out.println("Failed starting node " + n.getId());
				}
			}
			System.out.println("Done starting");
		}
	}
	
	public int getMinTimeouts() {
		int min = Integer.MAX_VALUE;
		for (Node n: this.allNodes.values()) {
			min = n.getTimeouts() < min ? n.getTimeouts() : min;
		}
		return min;
	}
	
	public int getMaxTimeouts() {
		int max = Integer.MIN_VALUE;
		for (Node n: this.allNodes.values()) {
			max = n.getTimeouts() > max ? n.getTimeouts() : max;
		}
		return max;
	}
	
	public double getAvgTimeouts() {
		int total = 0;
		for (Node n: this.allNodes.values())
			total += n.getTimeouts();
		return total * 1.0 / this.allNodes.size();
	}
	
	public int getFailures() {
		int total = 0;
		for (Node n: this.allNodes.values())
			total += n.getFailures();
		return total;
	}
	@ScheduledMethod(start = 1, priority = ScheduleParameters.LAST_PRIORITY, interval = 1)
	public void debug() {
		ArrayList<Integer> ids = new ArrayList<>(this.allNodes.keySet());
		Collections.sort(ids);
		for (Integer i: ids) {
			Node x = this.allNodes.get(i);
			int succ = x.successor.getId();
			int pred = x.predecessor == null ? -1 : x.predecessor.getId();
			System.out.println(x.getId() + " succ: " + succ + "; pred: " + pred);
		}
	}
}
