package org.hps;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Scaler {
    public static final String CONSUMER_GROUP = "testgroup2";
    public static  int NUM_PARTITIONS;
    static boolean  scaled = false;
    public static  AdminClient admin = null;
    private static final Logger log = LogManager.getLogger(Scaler.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //TODO creating a metaconsumer to enforce rebalance
       MetaDataConsumer consumer = new MetaDataConsumer();
       consumer.createDirectConsumer();
       consumer.consumerEnforceRebalance();


        Long sleep = Long.valueOf(System.getenv("SLEEP"));
        Long waitingTime = Long.valueOf(System.getenv("WAITING_TIME"));
        boolean firstIteration = true;
        log.info("sleep is {}", sleep);
        log.info("waiting time  is {}", waitingTime);


        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
        admin   = AdminClient.create(props);
        Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
        Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();

        Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
        Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();

        Map<TopicPartition, Long> partitionToLag = new HashMap<>();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();


        ///////////////////////////////////////////////////////////////////////////////////////

        while (true) {
            ListTopicsResult topics = admin.listTopics();
            topics.names().get().forEach(name -> log.info("topic name {}", name));


            log.info("Listing consumer groups, if any exist:");
            admin.listConsumerGroups().valid().get().forEach(name -> log.info("topic name {}\n", name));



            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                            .partitionsToOffsetAndMetadata().get();
            NUM_PARTITIONS = offsets.size();




            for (TopicPartition tp : offsets.keySet()) {
                requestLatestOffsets.put(tp, OffsetSpec.latest());
                requestEarliestOffsets.put(tp, OffsetSpec.earliest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    admin.listOffsets(requestLatestOffsets).all().get();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
                String topic = e.getKey().topic();
                int partition = e.getKey().partition();
                long committedOffset = e.getValue().offset();
                long latestOffset = latestOffsets.get(e.getKey()).offset();
                long lag = latestOffset - committedOffset;

                if (firstIteration) {

                    currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                    currentPartitionToLastOffset.put(e.getKey(), latestOffset);
                } else {

                    previousPartitionToCommittedOffset.put(e.getKey(),  currentPartitionToCommittedOffset.get(e.getKey()));
                    previousPartitionToLastOffset.put(e.getKey(),  currentPartitionToLastOffset.get(e.getKey()));
                    currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                    currentPartitionToLastOffset.put(e.getKey(), latestOffset);

                }
              //  currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                partitionToLag.put(e.getKey(), lag);
                log.info("Consumer group " + CONSUMER_GROUP
                        + " has committed offset " + committedOffset
                        + " to topic " + topic + " partition " + partition
                        + ". The latest offset in the partition is "
                        + latestOffset + " so consumer group is "
                        + (lag) + " records behind");
            }


            if (! firstIteration) {

                //logging
                for (Map.Entry<TopicPartition, Long> entry : previousPartitionToCommittedOffset.entrySet()) {
                    log.info("For partition {} previousCommittedOffsets = {}", entry.getKey(),
                            previousPartitionToCommittedOffset.get(entry.getKey()));
                    log.info(" For partition {} currentCommittedOffsets = {}", entry.getKey(),
                            currentPartitionToCommittedOffset.get(entry.getKey()));
                    //log latest produced offset
                    log.info("For partition {} previousEndOffsets = {}", entry.getKey(),
                            previousPartitionToLastOffset.get(entry.getKey()));
                    log.info(" For partition {} currentEndOffsets = {}", entry.getKey(),
                            currentPartitionToLastOffset.get(entry.getKey()));

                    log.info("partition {} has the following lag {}", entry.getKey().partition() , partitionToLag.get(entry.getKey()));
                }
            }




            log.info("Call to consumer group description ");

            Map<MemberDescription, Long> consumerToLag = new HashMap<>();
            Set<TopicPartition> topicPartitions;
             //lag per consumer
            Long lag = 0L;
            //get information on consumer groups, their partitions and their members
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singletonList(Scaler.CONSUMER_GROUP));
            KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                    describeConsumerGroupsResult.all();
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
           Map<String, Set<TopicPartition>> memberToTopicPartitionMap = new HashMap<>();

           //compute lag per consumer
            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                MemberAssignment memberAssignment = memberDescription.assignment();
                log.info("Member {} has the following assignments", memberDescription.consumerId());
                topicPartitions = memberAssignment.topicPartitions();
               memberToTopicPartitionMap.put(memberDescription.consumerId(), topicPartitions);
                for (TopicPartition tp : memberAssignment.topicPartitions()) {
                    log.info("\tpartition {}", tp.toString());
                     lag += partitionToLag.get(tp);
                }

                consumerToLag.putIfAbsent(memberDescription, lag);
                lag = 0L;
            }

          for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
             log.info("the total lag for partitions owned by  member {} is  {}", memberDescription.consumerId(),
                      consumerToLag.get(memberDescription));
          }




            int size =  consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();


            if (! firstIteration) {
                for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {

                    long totalpoff = 0;
                    long totalcoff = 0;
                    long totalepoff = 0;
                    long totalecoff = 0;

                    for(TopicPartition tp :  memberDescription.assignment().topicPartitions()) {
                        totalpoff += previousPartitionToCommittedOffset.get(tp);
                        totalcoff += currentPartitionToCommittedOffset.get(tp);
                        totalepoff += previousPartitionToLastOffset.get(tp);
                        totalecoff += currentPartitionToLastOffset.get(tp);
                    }

                    float consumptionratePerConsumer = (float) (totalcoff - totalpoff) / sleep;
                    float arrivalratePerConsumer = (float) (totalecoff - totalepoff) / sleep;


                    log.info("the consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                            consumptionratePerConsumer * 1000);
                    log.info("the arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                            arrivalratePerConsumer*1000);


                    if (consumerToLag.get(memberDescription) > (consumptionratePerConsumer * waitingTime)){
                        log.info("The magic formula for consumer {} does not hold I am going to scale by one for now",
                                memberDescription.consumerId());

                        if (size < NUM_PARTITIONS) {
                            log.info("consumers are less than nb partition we can scale");
                            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                                k8s.apps().deployments().inNamespace("default").withName("cons1pss").scale(size + 1);
                                // firstIteration = true;
                                scaled = true;
                                sleep = 2 * sleep;
                                break;
                            }
                        } else {
                            log.info("consumers are equal to nb partition we can not scale anymore");

                        }

                    } else {
                        log.info("the magic formula for consumer {} does  hold need NOT to scale, we shall downscale",
                                memberDescription.consumerId());

                        try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                            ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                            k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                            int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1pss").get().getSpec().getReplicas();
                            if (replicas > 1) {
                                k8s.apps().deployments().inNamespace("default").withName("cons1pss").scale(replicas - 1);
                                // firstIteration = true;
                                scaled = true;
                                sleep = 2 * sleep;
                                break;
                            }
                            else {
                                log.info("not going to downscale since we have only one replica");
                            }
                        }
                    }

                }
            }


            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                        consumerToLag.put(memberDescription, 0L);
            }

            firstIteration = false;
            Thread.sleep(sleep);


            if (scaled) {
                log.info("we already scaled so sleep twice {}", sleep);
                firstIteration = true;
                scaled = false;
                sleep = sleep/2;
            }

        }

    }


}














