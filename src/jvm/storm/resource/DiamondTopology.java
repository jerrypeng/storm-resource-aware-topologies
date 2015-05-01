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

public class DiamondTopology {
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
	public static class DiamondBolt extends BaseRichBolt {
	 	
	    OutputCollector _collector;
	    int time_loop=0;
	    int index=0;
	    long sleeptime=0;
	    public DiamondBolt(int time_loop,int index, long sleeptime) {
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
  public static class DiamondSpout extends BaseRichSpout {
	    SpoutOutputCollector _collector;
	    int time_loop=0;
	    int index=0;
	    long sleeptime=0;
	    public DiamondSpout(int time_loop,int index, long sleeptime) {
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
		int paralellism = 8;

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer spout = builder.setSpout("spout_head", new DiamondSpout(1, 4, 5), 6);
        spout.setCPULoad(50.0);

       BoltDeclarer bolt1 = builder.setBolt("bolt_1", new DiamondBolt(1,2,5), 3);
       BoltDeclarer bolt2 = builder.setBolt("bolt_2", new DiamondBolt(1,2,5), 3);
       BoltDeclarer bolt3 = builder.setBolt("bolt_3", new DiamondBolt(1,2,5), 3);
       BoltDeclarer bolt4 = builder.setBolt("bolt_4", new DiamondBolt(1,2,5), 3);
       
       bolt1.shuffleGrouping("spout_head");
       bolt1.setCPULoad(20.0);
       
       bolt2.shuffleGrouping("spout_head");
       bolt2.setCPULoad(20.0);
       
       bolt3.shuffleGrouping("spout_head");
       bolt3.setCPULoad(20.0);
       
       bolt4.shuffleGrouping("spout_head");
       bolt4.setCPULoad(20.0);

        BoltDeclarer output = builder.setBolt("bolt_output_3", new DiamondBolt(1,2,5), 6);
        output.shuffleGrouping("bolt_1");
        output.shuffleGrouping("bolt_2");
        output.shuffleGrouping("bolt_3");
        output.shuffleGrouping("bolt_4");
        output.setCPULoad(20.0);

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);

         conf.setNumAckers(0);

        conf.setNumWorkers(12);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        builder.createTopology());

	}

}