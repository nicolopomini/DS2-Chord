package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import repast.simphony.context.Context;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.engine.schedule.ScheduledMethod;

public class NodeManager {
	private HashMap<Integer, Node> allNodes;
	private double failProb;
	private Context<Object> context;
	
	public NodeManager(Context<Object> context, HashMap<Integer, Node> allNodes, double failProb) {
		this.allNodes = allNodes;
		this.failProb = failProb;
		this.context = context;
	}
	
	@ScheduledMethod(start = 1, priority = ScheduleParameters.FIRST_PRIORITY)
	public void killNodes() {
		ArrayList<Node> nodes = new ArrayList<>(this.allNodes.values());
		Collections.shuffle(nodes);
		int upTo = (int)(this.failProb * nodes.size());
		for (int i = 0; i < upTo; i++) {
			nodes.get(i).fail();
			context.remove(nodes.get(i));
			allNodes.remove(nodes.get(i).getId());
		}
		System.out.println(upTo + " nodes have failed");
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
