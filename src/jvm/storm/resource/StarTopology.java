package storm.resource;

import java.util.Map;
import java.util.Random;

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
	public static boolean isPrime(int n) 
	  {
	    boolean ret=true;
	    for(int i=2;i<n;i++) {
	        if(n%i==0)
	            ret=false;
	    }
	    return ret;
	  }
	  
	  public static int PrimeSearch(int time_loop,int index, long sleeptime)
	  {
	    int max=2;
	    //find the largest prime number within [1,input]
	    // percentage index up to 70% percentage = 10*index
	    int[] cpu_para = new int[] {65,65,65,120,190,260,440,660};
	    for(int j=0; j<time_loop ; j++)
	    {
	      for(int i =300; i<2000; i++)
	      {
	          if(i%cpu_para[index]==0)
	          {
	            try {
	                Thread.sleep(sleeptime);
	            } 
	            catch (InterruptedException ie) {
	                   //Handle exception
	            }
	          }

	          if(isPrime(i))
	              max=i;
	      }
	    }
	    return max;
	  }
	public static class StarBolt extends BaseRichBolt {
	 	
	    OutputCollector _collector;
	    int time_loop=0;
	    int index=0;
	    long sleeptime=0;
	    public StarBolt(int time_loop,int index, long sleeptime) {
	 		this.time_loop = time_loop;
	 		this.index = index;
	 	}
	    @Override
	    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	        _collector = collector;
	    }
	    @Override
	    public void execute(Tuple tuple) {
	    	PrimeSearch(this.time_loop, this.index, this.sleeptime);
	        _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
	        //_collector.ack(tuple);
	    }
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("word"));
	    }
	  }
public static class StarSpout extends BaseRichSpout {
	    SpoutOutputCollector _collector;
	    int time_loop=0;
	    int index=0;
	    long sleeptime=0;
	    public StarSpout(int time_loop,int index, long sleeptime) {
	 		this.time_loop = time_loop;
	 		this.index = index;
	 	}
	    @Override
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	      _collector = collector;
	    }
	    @Override
	    public void nextTuple() {
	    	PrimeSearch(this.time_loop, this.index, this.sleeptime);
	        _collector.emit(new Values("Jerry"));
	        Utils.sleep(100);
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
		int numSpout = 2;
		int numBolt = 2;
		int paralellism = 4;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new StarBolt(1,2,5),
				12);
		center.setCPULoad(20.0);

		for (int i = 0; i < numSpout; i++) {
			SpoutDeclarer spout = builder.setSpout("spout_" + i, new StarSpout(1, 4, 1), 8);
			center.shuffleGrouping("spout_" + i);
			spout.setCPULoad(50.0);
		}

		for (int i = 0; i < numBolt; i++) {
			BoltDeclarer bolt = builder.setBolt("bolt_output_" + i, new StarBolt(1,2,5), 6)
					.shuffleGrouping("center");
			bolt.setCPULoad(20.0);
		}
		
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);
		
		//conf.setNumAckers(0);

		conf.setNumWorkers(12);
		
	

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}
}