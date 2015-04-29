package storm.resource;

import java.util.Map;

import storm.resource.bolt.TestBolt;
import storm.resource.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StarTopology {
	public static void doWork(int level) {
		for(int i=0; i<level; i++){
			double sum = 1000.0/34.0*232.0;
		}
		
	}
	
	public static class StarSpout extends BaseRichSpout {
	    SpoutOutputCollector _collector;
	    private int level=0;
	    public StarSpout(int level) {
	    	this.level = level;
	    }
	    
	    @Override
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	      _collector = collector;
	    }
	    @Override
	    public void nextTuple() {
	        doWork(this.level);
	        Utils.sleep(10);
	        _collector.emit(new Values("Jerry"));
	    }
	    @Override
	    public void ack(Object id) {
	    }
	    @Override
	    public void fail(Object id) {
	    }
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("word"));
	    }
	  }
	public static class StarBolt extends BaseRichBolt {
	    OutputCollector _collector;
	    private int level=0;
	    public StarBolt(int level) {
	    	this.level = level;
	    }
	    @Override
	    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	        _collector = collector;
	    }
	    @Override
	    public void execute(Tuple tuple) {
	    	doWork(this.level);
	        _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
	        //_collector.ack(tuple);
	    }
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("word"));
	    }
	  }
	
	public static void main(String[] args) throws Exception {
		int numSpout = 2;
		int numBolt = 2;
		int paralellism = 3*2*2;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new StarBolt(100),
				paralellism*2);
		center.setCPULoad(25.0);

		for (int i = 0; i < numSpout; i++) {
			SpoutDeclarer spout = builder.setSpout("spout_" + i, new StarSpout(1000), paralellism);
			center.shuffleGrouping("spout_" + i);
			spout.setCPULoad(40.0);
		}

		for (int i = 0; i < numBolt; i++) {
			BoltDeclarer bolt = builder.setBolt("bolt_output_" + i, new StarBolt(100), paralellism)
					.shuffleGrouping("center");
			bolt.setCPULoad(15.0);
		}
		
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);
		
		conf.setNumAckers(0);

		conf.setNumWorkers(12);
		
	

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}
}