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
	 //cpu workload 
	  public static boolean isPrime(int n) 
	  {
	    boolean ret=true;
	    for(int i=2;i<n;i++) {
	        if(n%i==0)
	            ret=false;
	    }
	    return ret;
	  }
	  public static int PrimeSearch(int time_loop,int index)
	  {
	    int max=2;
	    //find the largest prime number within [1,input]
	    // percentage index up to 70% percentage = 10*index
	    int[] cpu_para = new int[] {35,35,35,90,190,260,440,660};
	    for(int j=0; j<time_loop ; j++)
	    {
	      for(int i =300; i<2000; i++)
	      {
	          if(i%cpu_para[index]==0)
	          {
	            try {
	                Thread.sleep(5);
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
	 public static class TestTopologyBolt2 extends BaseRichBolt {
		 	
		    OutputCollector _collector;
		    int time_loop=0;
		    int index=0;
		    public TestTopologyBolt2(int time_loop,int index) {
		 		this.time_loop = time_loop;
		 		this.index = index;
		 	}
		    @Override
		    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		        _collector = collector;
		    }
		    @Override
		    public void execute(Tuple tuple) {
		    	PrimeSearch(this.time_loop, this.index);
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
	 public static class TestTopologySpout2 extends BaseRichSpout {
		    SpoutOutputCollector _collector;
		    int time_loop=0;
		    int index=0;
		    public TestTopologySpout2(int time_loop,int index) {
		 		this.time_loop = time_loop;
		 		this.index = index;
		 	}
		    @Override
		    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		      _collector = collector;
		    }
		    @Override
		    public void nextTuple() {
		    	PrimeSearch(this.time_loop, this.index);
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

        SpoutDeclarer spout = builder.setSpout("spout_head_1", new TestTopologySpout(Integer.parseInt(args[1])), paralellism);
        spout.setCPULoad(50.0);

        BoltDeclarer output = builder.setBolt("bolt_output_1", new TestTopologyBolt(Integer.parseInt(args[1])), paralellism);
        output.shuffleGrouping("spout_head_1");
        
        SpoutDeclarer spout2 = builder.setSpout("spout_head_2", new TestTopologySpout2(Integer.parseInt(args[2]), Integer.parseInt(args[3])), paralellism);
        spout.setCPULoad(50.0);

        BoltDeclarer output2 = builder.setBolt("bolt_output_2", new TestTopologyBolt2(Integer.parseInt(args[2]), Integer.parseInt(args[3])), paralellism);
        output.shuffleGrouping("spout_head_2");
       
       
        output.setCPULoad(15.0);

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);

         conf.setNumAckers(0);

        conf.setNumWorkers(4);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        builder.createTopology());

	}

}
