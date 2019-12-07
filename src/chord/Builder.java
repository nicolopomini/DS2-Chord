package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import repast.simphony.context.Context;
import repast.simphony.context.space.continuous.ContinuousSpaceFactory;
import repast.simphony.context.space.continuous.ContinuousSpaceFactoryFinder;
import repast.simphony.context.space.graph.NetworkBuilder;
import repast.simphony.dataLoader.ContextBuilder;
import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.parameter.Parameters;
import repast.simphony.random.RandomHelper;
import repast.simphony.space.continuous.ContinuousSpace;
import repast.simphony.space.continuous.RandomCartesianAdder;
import repast.simphony.space.graph.Network;

public class Builder implements ContextBuilder<Object> {
	@Override
	public Context build(Context<Object> context) {
		context.setId("Chord");
		NetworkBuilder<Object> netBuilder = new NetworkBuilder<Object>(
				"chord network", context, true);
		netBuilder.buildNetwork();

		ContinuousSpaceFactory spaceFactory = ContinuousSpaceFactoryFinder
				.createContinuousSpaceFactory(null);
		ContinuousSpace<Object> space = spaceFactory.createContinuousSpace(
				"space", context, new RandomCartesianAdder<Object>(),
				new repast.simphony.space.continuous.WrapAroundBorders(), 50,
				50);
		Network<Object> net = (Network<Object>)context.getProjection("chord network");
		Parameters params = RunEnvironment.getInstance().getParameters();
		int keysExponent = params.getInteger("keys_exponent");
		int nodes = params.getInteger("nodes");
		int rounds = params.getInteger("rounds");
		float failureProb = params.getFloat("fail_prob");
		int succLength = params.getInteger("successor_length");
		String runType = params.getString("type");
		int failJoin = params.getInteger("join-fail");
		assert (failureProb <= 1.0);
		if (RunEnvironment.getInstance().isBatch()) {
			keysExponent = nodes + 7;
			nodes = (int)Math.pow(2, nodes);
		}
		Node.M = keysExponent;
		int totalKeys = (int)Math.pow(2, keysExponent);
		System.out.println("Total nodes " + nodes);
		System.out.println("Total keys " + totalKeys);
		// setting positions
		double spaceSize = space.getDimensions().getHeight();
		double center = spaceSize / 2;
		double radius = center - 2;
		// setting nodes
		HashMap<Integer, Node> allNodes = new HashMap<>();
		for (int i = 0; i < nodes; i++) {
			int id = RandomHelper.nextIntFromTo(i * totalKeys / nodes, (i + 1) * totalKeys / nodes - 1);
			Node n = new Node(id, net);
			allNodes.put(id, n);
			context.add(n);
			double theta = 2 * Math.PI * id / totalKeys;
	        double x = center + radius * Math.cos(theta);
	        double y = center + radius * Math.sin(theta);
	        space.moveTo(n, x, y);
		}
		// setting finger tables
		ArrayList<Integer> allIds = new ArrayList<>(allNodes.keySet());
		Collections.sort(allIds);
		for (Node n: allNodes.values()) {
			ArrayList<Node> finger = new ArrayList<>(keysExponent);
			for (int i = 0; i < keysExponent; i++) {
				int realIndex = (n.getId() + (int)Math.pow(2, i)) % totalKeys;
				boolean added = false;
				for (Integer index: allIds) {
					if (index >= realIndex) {
						finger.add(allNodes.get(index));
						added = true;
						break;
					}
				}
				if (!added)
					finger.add(allNodes.get(allIds.get(0)));
			}
			ArrayList<Node> successors = new ArrayList<>(succLength);
			int index = allIds.indexOf(n.getId());
			index = (index + 1) % allIds.size();
			for (int z = 0; z < succLength; z++) {
				successors.add(allNodes.get(allIds.get(index)));
				index = (index + 1) % allIds.size();
			}
			n.setFingerTable(finger);
			n.setSuccessors(successors);
			net.addEdge(n, successors.get(0));
		}
		// set predecessor
		for (int i = 0; i < allIds.size(); i++) {
			int index = i - 1;
			if (index < 0)
				index = allIds.size() - 1;
			allNodes.get(allIds.get(i)).setPredecessor(allNodes.get(allIds.get(index)));
		}
		NodeManager manager = new NodeManager(context, space, net, allNodes, failureProb, runType.equals("Disaster"), failJoin, totalKeys, center, radius);
		context.add(manager);
		RunEnvironment.getInstance().endAt(rounds);
		return context;
	}
}
