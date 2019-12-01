package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import repast.simphony.context.Context;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.engine.schedule.ScheduledMethod;
import repast.simphony.random.RandomHelper;
import repast.simphony.space.continuous.ContinuousSpace;

public class NodeManager {
	private HashMap<Integer, Node> allNodes;
	private double failProb, center, radius;
	private Context<Object> context;
	private ContinuousSpace<Object> space;
	private boolean disasterMode, done;
	private int joinLeave, totalKeys;
	
	public NodeManager(Context<Object> context, ContinuousSpace<Object> space, HashMap<Integer, Node> allNodes, double failProb, boolean disasterMode, int joinLeave, int totalKeys, double center, double radius) {
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
	}
	
	@ScheduledMethod(start = 1, priority = ScheduleParameters.FIRST_PRIORITY, interval = 1)
	public void killNodes() {
		if (this.disasterMode && !this.done) {
			ArrayList<Node> nodes = new ArrayList<>(this.allNodes.values());
			Collections.shuffle(nodes);
			int upTo = (int)(this.failProb * nodes.size());
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
			for (int i = 0; i < this.joinLeave; i++) {
				nodes.get(i).leave();
				context.remove(nodes.get(i));
				allNodes.remove(nodes.get(i).getId());
			}
			System.out.println("Done failing");
//			nodes = new ArrayList<>(this.allNodes.values());
//			for (int i = 0; i < this.joinLeave; i++) {
//				int nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
//				while (this.allNodes.containsKey(nextKey))
//					nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
//				Node randomKnown = nodes.get(RandomHelper.nextIntFromTo(0, nodes.size() - 1));
//				//System.out.println("Random null: " + (randomKnown == null));
//				//System.out.println("Starting node " + nextKey);
//				Node n = new Node(nextKey);
//				allNodes.put(nextKey, n);
//				context.add(n);
//				n.join(randomKnown);
////				while (!n.join(randomKnown)) {
////					System.out.println("Join has failed, trying again...");
////					nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
////					while (this.allNodes.containsKey(nextKey))
////						nextKey = RandomHelper.nextIntFromTo(0, this.totalKeys - 1);
////					randomKnown = nodes.get(RandomHelper.nextIntFromTo(0, nodes.size() - 1));
////					//n = new Node(nextKey);
////				}
//				double theta = 2 * Math.PI * nextKey / totalKeys;
//		        double x = center + radius * Math.cos(theta);
//		        double y = center + radius * Math.sin(theta);
//		        space.moveTo(n, x, y);
//		        System.out.println("Node " + nextKey + " started");
////				if (n.join(randomKnown) == 0) {
////					allNodes.put(nextKey, n);
////					context.add(n);
////					double theta = 2 * Math.PI * nextKey / totalKeys;
////			        double x = center + radius * Math.cos(theta);
////			        double y = center + radius * Math.sin(theta);
////			        space.moveTo(n, x, y);
////			        System.out.println("Node started");
////				} else
////					System.out.println("Node not started since join has failed");
//			}
//			System.out.println("Done starting");
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
}
