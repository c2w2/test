package test;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.time.*;;

public class tat {
	private static final int NUM_THREADS = 1;
	 private static Calendar date;
	 static boolean isTiming=false;
	 static int time=0,MAX_ALLOWED=60, maxallowcount=100;
	  static String topic = "tail";
	
	  public static void main(String[] args) throws Exception {
			 
			Properties props = new Properties();
	       	props.put("group.id", "test-group");
	       	props.put("zookeeper.connect", "kafka1:2181,kafka2:2181,kafka3:2181");
	        props.put("auto.commit.interval.ms", "1000");
	        ConsumerConfig consumerConfig = new ConsumerConfig(props);
	        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	          topic += args[0];
	          topicCountMap.put(topic, NUM_THREADS);
	    	Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

	    	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
	          int k=0;
	        final HashMap<String, Integer>ip = new HashMap<String, Integer>();
	        long start = System.currentTimeMillis();
	        long end = System.currentTimeMillis() ;
	        final String iptmp[] = new String[9999];
	        final int iptmpa[]={0};
	        while(true)
	        {
	        if((end-start)/1000<MAX_ALLOWED)
	        {
	        	for (final KafkaStream<byte[], byte[]> stream : streams) { 
	        		executor.execute(new Runnable() { 
	        	        
	        	       			public synchronized void run() { 
	        	      				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) { 
	        	      					String tmp = new String(messageAndMetadata.message()); 
	        	      					
	        	      					if(ip.containsKey(tmp))
	        	      					{
	        	      						int itmp = (int)ip.get(tmp);
 	        	      						ip.put(tmp, itmp+1);
	        	      					}else
	        	      					{
	        	      						ip.put(tmp, 1);
	        	      						iptmp[iptmpa[0]++]=tmp;
	        	      					}
	        	       			 
	        	       					         	       					
	        	        					 
	        	      				} 
	        	        				
	        	     
	        	       			}
	        	       			});
	        	        	}
 	
	        	
	        }else
	        { 
	        	for(int i=0; i<iptmpa[0]; i++)
	        	{
	        		if((int)ip.get(iptmp[i])>maxallowcount)
	        		{
	        			System.out.println("+---------------[ERROR]--------------+");
	        			System.out.println("| 								     |");
	        			System.out.println("| "+iptmp[i]+" has been Detected!"+" |");
	        			System.out.println("| 								     |");
	        			System.out.println("+------------------------------------+");
	        		}
	        	}
	        }
	        end = System.currentTimeMillis();
	      }
	 
}
}
