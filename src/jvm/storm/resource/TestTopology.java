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

public class TestTopology {
	public static void doWork(int level) {
		recFibN(level);
		Utils.sleep(5);
	}
	public static long recFibN(final int n)
	{
	 return (n < 2) ? n : recFibN(n - 1) + recFibN(n - 2);
	}
	 public static class TestTopologyBolt extends BaseRichBolt {
		 	
		    OutputCollector _collector;
		    int level=0;
		    public TestTopologyBolt(int level) {
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
	 public static class TestTopologySpout extends BaseRichSpout {
		    SpoutOutputCollector _collector;
		    int level=0;
		    public TestTopologySpout(int level) {
		 		this.level = level;
		 	}
		    @Override
		    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		      _collector = collector;
		    }
		    @Override
		    public void nextTuple() {
		        doWork(this.level);
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
	public static void main(String[] args) throws Exception {
		int paralellism = 1;

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer spout = builder.setSpout("spout_head", new TestTopologySpout(10), paralellism);
        spout.setCPULoad(50.0);

        BoltDeclarer output = builder.setBolt("bolt_output_3", new TestTopologyBolt(10), paralellism);
        output.shuffleGrouping("spout_head");
       
        output.setCPULoad(15.0);

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);

         conf.setNumAckers(0);

        conf.setNumWorkers(2);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        builder.createTopology());

	}

}
