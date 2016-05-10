package iot_framework_big_data.kafka_storm_mongodb;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class IotKafkaMongoTopology {
	public static final Logger LOG = LoggerFactory.getLogger(IotKafkaMongoTopology.class);
	
	public static class MongoInsertBolt extends BaseBasicBolt{
		public void declareOutputFields(OutputFieldsDeclarer declarer){
			
		}
		
		public void execute(Tuple tuple, BasicOutputCollector collector){
			LOG.info(tuple.toString());
		}
	}
	
	private StaticHosts hosts;
	
	public IotKafkaMongoTopology(String brokerHostPort, int partition){
		Broker broker = new Broker("52.70.166.27");
		GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation("TEMPERATURE");
		partitionInfo.addPartition(partition, broker);
		hosts = new StaticHosts(partitionInfo);
	}
	
	public StormTopology buildTopology(){
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, "TEMPERATURE", "/brokers", "storm");
		kafkaConfig.zkServers = ImmutableList.of("52.70.166.27");
		kafkaConfig.zkPort = 2181;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaspout", new KafkaSpout(kafkaConfig), 10);
		builder.setBolt("mongobolt", new MongoInsertBolt()).shuffleGrouping("kafkaspout");
		return builder.createTopology();
	}
	
	public static void main (String args[]){
		String kafkaHost = "52.70.166.27";
		IotKafkaMongoTopology top = new IotKafkaMongoTopology(kafkaHost, 0);
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
		StormTopology stormTopology = top.buildTopology();
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(2);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka", conf, stormTopology);
	}
}
