import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

/** Runs against existing RocketMQ client jars; does not implement the wire protocol. */
@SuppressWarnings("deprecation")
public final class JavaInterop {
    private static final String KEY = "interop-key";

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2 || args[0].trim().isEmpty()) {
            throw new IllegalArgumentException("Usage: JavaInterop <namesrv-host:port> [brokerName=broker-a]");
        }
        String brokerName = args.length == 2 ? args[1] : "broker-a";
        require(!brokerName.trim().isEmpty(), "brokerName must not be empty");
        String suffix = System.currentTimeMillis() + "_" + Long.toUnsignedString(System.nanoTime());
        String topic = "RustInterop_" + suffix;
        MessageQueue queue = new MessageQueue(topic, brokerName, 0);
        DefaultMQProducer producer = new DefaultMQProducer("JavaInteropProducer_" + suffix);
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("JavaInteropConsumer_" + suffix);
        producer.setNamesrvAddr(args[0]);
        consumer.setNamesrvAddr(args[0]);
        producer.setVipChannelEnabled(false);
        consumer.setVipChannelEnabled(false);
        producer.setInstanceName("JavaInterop_" + suffix);
        consumer.setInstanceName("JavaInterop_" + suffix);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setAutoBatch(false);
        producer.setCompressType(CompressionType.ZLIB);
        producer.setSendMsgTimeout(10000);
        consumer.setEnableRebalance(false);

        byte[][] bodies = {
            utf8("hello-from-java"), utf8("batch-from-java-1"), utf8("batch-from-java-2")
        };
        String[] tags = {"blue", "blue", "green"};
        System.out.println("JAVA_INTEROP_BEGIN namesrv=" + args[0] + " queue=" + queue);
        try {
            require("TBW102".equals(producer.getCreateTopicKey()), "default topic key must be TBW102");
            producer.start();
            // No explicit createTopic: the first send must discover TBW102 and auto-create the topic.
            SendResult single = producer.send(new Message(topic, tags[0], KEY, bodies[0]), queue);
            checkSend("sync", single, queue, 0);
            SendResult batch = producer.send(Arrays.asList(
                new Message(topic, tags[1], KEY, bodies[1]),
                new Message(topic, tags[2], KEY, bodies[2])), queue);
            checkSend("batch", batch, queue, 1);
            require(batch.getMsgId().split(",").length == 2, "batch must return two client message IDs: " + batch);

            consumer.start();
            require(consumer.getOffsetStore() instanceof RemoteBrokerOffsetStore,
                "offset verification must use broker storage, not a local file");
            DefaultMQPullConsumerImpl impl = consumer.getDefaultMQPullConsumerImpl();
            // Persistence only includes queues assigned to the consumer.
            impl.getRebalanceImpl().getProcessQueueTable().put(queue, new ProcessQueue());

            PullResult all = consumer.pull(queue, "*", 0, 32);
            List<MessageExt> messages = checkPull("pull all", all, 3, 3, 3);
            for (int i = 0; i < bodies.length; i++) {
                checkMessage("pull all[" + i + "]", messages.get(i), queue, i, tags[i], bodies[i]);
                require((messages.get(i).getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == 0,
                    "small message must not be compressed at offset " + i);
            }
            System.out.println("JAVA_INTEROP_PULL_OK count=3 offsets=0,1,2");

            PullResult blue = consumer.pull(queue, "blue", 0, 32);
            List<MessageExt> blueMessages = checkPull("pull blue", blue, 2, 3, 3);
            checkMessage("blue[0]", blueMessages.get(0), queue, 0, "blue", bodies[0]);
            checkMessage("blue[1]", blueMessages.get(1), queue, 1, "blue", bodies[1]);
            // NO_MATCHED_MSG distinguishes server filtering from client-side filtering.
            checkNoMatch("blue excludes green", consumer.pull(queue, "blue", 2, 32), 3);
            System.out.println("JAVA_INTEROP_TAG_FILTER_OK");

            consumer.updateConsumeOffset(queue, 3);
            impl.persistConsumerOffset();
            // Persistence uses a one-way RPC; only the broker read is retried, never the write.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            long storedOffset;
            do {
                storedOffset = consumer.fetchConsumeOffset(queue, true);
                if (storedOffset == 3 || System.nanoTime() >= deadline) {
                    break;
                }
                Thread.sleep(20);
            } while (true);
            equal("fetchConsumeOffset(fromStore=true)", 3, storedOffset);
            System.out.println("JAVA_INTEROP_CONSUMER_OFFSET_OK offset=" + storedOffset);

            equal("minOffset", 0, consumer.minOffset(queue));
            equal("maxOffset", 3, consumer.maxOffset(queue));
            long firstTimestamp = messages.get(0).getStoreTimestamp();
            long lastTimestamp = messages.get(2).getStoreTimestamp();
            require(firstTimestamp > 0 && lastTimestamp >= firstTimestamp, "invalid broker store timestamps");
            equal("searchOffset(before first)", 0, consumer.searchOffset(queue, 0));
            equal("searchOffset(first timestamp)", 0, consumer.searchOffset(queue, firstTimestamp));
            equal("searchOffset(after last)", 3, consumer.searchOffset(queue, lastTimestamp + 1));
            System.out.println("JAVA_INTEROP_OFFSET_QUERIES_OK min=0 max=3");

            // The direct view API avoids fallback to a unique-key index query.
            MessageExt viewed = producer.getDefaultMQProducerImpl().viewMessage(topic, single.getOffsetMsgId());
            checkMessage("viewMessage", viewed, queue, 0, "blue", bodies[0]);
            equal("viewMessage physical offset", messages.get(0).getCommitLogOffset(), viewed.getCommitLogOffset());
            System.out.println("JAVA_INTEROP_VIEW_OK msgId=" + single.getMsgId()
                + " offsetMsgId=" + single.getOffsetMsgId());

            StringBuilder largeText = new StringBuilder();
            for (int i = 0; i < 512; i++) {
                largeText.append("hello-from-java-compressed-\u4e2d\u6587-").append(i).append('\n');
            }
            byte[] largeBody = utf8(largeText.toString());
            require(largeBody.length > 4096 && largeBody.length > producer.getCompressMsgBodyOverHowmuch(),
                "large body must exceed the client's compression threshold");
            SendResult compressed = producer.send(new Message(topic, "red", KEY, largeBody), queue);
            checkSend("compressed", compressed, queue, 3);
            PullResult last = consumer.pull(queue, "*", 3, 32);
            MessageExt decoded = checkPull("pull compressed", last, 1, 4, 4).get(0);
            checkMessage("compressed body", decoded, queue, 3, "red", largeBody);
            // MessageDecoder clears COMPRESSED_FLAG after inflating the stored body.
            require((decoded.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == 0,
                "Java decoder must clear the compression flag after decompression");
            checkNoMatch("blue excludes compressed red", consumer.pull(queue, "blue", 3, 32), 4);
            equal("maxOffset after compressed send", 4, consumer.maxOffset(queue));
            equal("pull must not implicitly commit offset", 3, consumer.fetchConsumeOffset(queue, true));
            System.out.println("JAVA_INTEROP_COMPRESSION_OK bytes=" + largeBody.length + " offset=3");

            MessageQueue scheduleQueue = new MessageQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, brokerName, 0);

            // A delay level parks the record in the schedule queue; only the delivery engine makes
            // it readable on the real queue, one second later for level 1.
            byte[] delayedBody = utf8("delayed-from-java");
            long scheduleOffset = consumer.maxOffset(scheduleQueue);
            Message delayed = new Message(topic, "yellow", KEY, delayedBody);
            delayed.setDelayTimeLevel(1);
            SendResult parked = producer.send(delayed, queue);
            require(parked != null && parked.getSendStatus() == SendStatus.SEND_OK, "delayed: " + parked);
            equal("delay level 1 must stay parked", 4, consumer.maxOffset(queue));
            waitFor("delayed record parked in the schedule queue",
                () -> consumer.maxOffset(scheduleQueue), scheduleOffset + 1);
            waitFor("delayed record delivered", () -> consumer.maxOffset(queue), 5);
            MessageExt released = checkPull("pull delayed", consumer.pull(queue, "*", 4, 32), 1, 5, 5).get(0);
            checkMessage("delayed", released, queue, 4, "yellow", delayedBody);
            require(released.getDelayTimeLevel() == 0, "delivered message lost its delay level: " + released);
            System.out.println("JAVA_INTEROP_DELAY_OK level=1 offset=4");

            // Consumer send back re-parks the record, so the retry topic receives it from the
            // schedule engine with the reconsume time and origin ID the broker added.
            String consumerGroup = consumer.getConsumerGroup();
            MessageQueue retryQueue = new MessageQueue(MixAll.getRetryTopic(consumerGroup), brokerName, 0);
            MessageQueue dlqQueue = new MessageQueue(MixAll.getDLQTopic(consumerGroup), brokerName, 0);
            scheduleOffset = consumer.maxOffset(scheduleQueue);
            consumer.sendMessageBack(messages.get(1), 1);
            waitFor("send back parked in the schedule queue",
                () -> consumer.maxOffset(scheduleQueue), scheduleOffset + 1);
            MessageExt retried = checkPull("retry delivery",
                waitForPull("retry delivery", consumer, retryQueue), 1, 1, 1).get(0);
            checkMessage("retry", retried, retryQueue, 0, "blue", bodies[1]);
            equal("retry reconsumeTimes", 1, retried.getReconsumeTimes());
            require(retried.getDelayTimeLevel() == 0, "retry message lost its delay level: " + retried);
            require(topic.equals(retried.getProperty(MessageConst.PROPERTY_RETRY_TOPIC)),
                "retry topic property: " + retried.getProperty(MessageConst.PROPERTY_RETRY_TOPIC));
            require(messages.get(1).getMsgId().equals(retried.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID)),
                "origin message ID: " + retried.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID));
            System.out.println("JAVA_INTEROP_SEND_BACK_OK retry=" + retryQueue.getTopic());

            // A negative delay level is the consumer asking for the dead letter queue directly,
            // which bypasses the schedule queue entirely.
            long beforeDlq = consumer.maxOffset(scheduleQueue);
            consumer.sendMessageBack(messages.get(2), -1);
            MessageExt dead = checkPull("dead letter", waitForPull("dead letter", consumer, dlqQueue),
                1, 1, 1).get(0);
            checkMessage("dead letter", dead, dlqQueue, 0, "green", bodies[2]);
            require(dead.getDelayTimeLevel() == 0, "dead letter message lost its delay level: " + dead);
            equal("dead letter reconsumeTimes", 1, dead.getReconsumeTimes());
            equal("send back to the dead letter queue must not park", beforeDlq,
                consumer.maxOffset(scheduleQueue));
            System.out.println("JAVA_INTEROP_DLQ_OK dlq=" + dlqQueue.getTopic());

            checkSubscriptionGroups(args[0], topic, suffix, brokerName);
        } finally {
            try {
                consumer.shutdown();
            } finally {
                producer.shutdown();
            }
        }
        System.out.println("JAVA_INTEROP_OK");
    }

    /**
     * Subscription group administration over the remoting protocol: create, read back, delete.
     * The retry policy and queue counts a Java admin writes must survive the broker round trip.
     */
    private static void checkSubscriptionGroups(String namesrv, String topic, String suffix, String brokerName)
        throws Exception {
        String adminGroup = "JavaInteropGroup_" + suffix;
        DefaultMQAdminExt admin = new DefaultMQAdminExt("JavaInteropAdmin_" + suffix);
        admin.setNamesrvAddr(namesrv);
        admin.setVipChannelEnabled(false);
        admin.setInstanceName("JavaInteropAdmin_" + suffix);
        admin.start();
        try {
            String brokerAddr = admin.examineTopicRouteInfo(topic).getBrokerDatas().get(0)
                .getBrokerAddrs().get(MixAll.MASTER_ID);
            require(brokerAddr != null && !brokerAddr.isEmpty(), "route info has no master broker address");

            SubscriptionGroupConfig config = new SubscriptionGroupConfig();
            config.setGroupName(adminGroup);
            config.setRetryQueueNums(2);
            config.setRetryMaxTimes(5);
            config.getGroupRetryPolicy().setType(GroupRetryPolicyType.EXPONENTIAL);
            config.getGroupRetryPolicy().setExponentialRetryPolicy(new ExponentialRetryPolicy(1000, 60000, 3));
            admin.createAndUpdateSubscriptionGroupConfig(brokerAddr, config);

            SubscriptionGroupConfig stored = admin.examineSubscriptionGroupConfig(brokerAddr, adminGroup);
            require(stored != null, "group " + adminGroup + " is missing after it was created");
            equal("stored retryQueueNums", 2, stored.getRetryQueueNums());
            equal("stored retryMaxTimes", 5, stored.getRetryMaxTimes());
            require(stored.isConsumeEnable(), "consumeEnable must default to true");
            equal("default consumeTimeoutMinute", 15, stored.getConsumeTimeoutMinute());
            require(GroupRetryPolicyType.EXPONENTIAL == stored.getGroupRetryPolicy().getType(),
                "retry policy type: " + stored.getGroupRetryPolicy().getType());
            ExponentialRetryPolicy exponential = stored.getGroupRetryPolicy().getExponentialRetryPolicy();
            require(exponential != null, "the exponential retry policy was dropped");
            equal("retry policy initial", 1000, exponential.getInitial());
            equal("retry policy max", 60000, exponential.getMax());
            equal("retry policy multiplier", 3, exponential.getMultiplier());

            admin.deleteSubscriptionGroup(brokerAddr, adminGroup, true);
            require(admin.examineSubscriptionGroupConfig(brokerAddr, adminGroup) == null,
                "group " + adminGroup + " survived deletion");
            System.out.println("JAVA_INTEROP_GROUP_ADMIN_OK group=" + adminGroup);
        } finally {
            admin.shutdown();
        }
    }

    private static long waitFor(String label, Reading reading, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            long actual = reading.get();
            if (actual == expected) {
                return actual;
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("JAVA_INTEROP_FAILED: " + label + " timed out at " + actual
                    + ", expected " + expected);
            }
            Thread.sleep(50);
        }
    }

    private static PullResult waitForPull(String label, DefaultMQPullConsumer consumer, MessageQueue queue)
        throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            PullResult result = consumer.pull(queue, "*", 0, 32);
            if (result != null && result.getPullStatus() == PullStatus.FOUND) {
                return result;
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("JAVA_INTEROP_FAILED: " + label + " found no message: " + result);
            }
            Thread.sleep(50);
        }
    }

    private interface Reading {
        long get() throws Exception;
    }

    private static void checkSend(String label, SendResult result, MessageQueue queue, long offset) {
        require(result != null, label + ": null SendResult");
        require(result.getSendStatus() == SendStatus.SEND_OK, label + ": " + result);
        require(queue.equals(result.getMessageQueue()), label + ": wrong message queue: " + result);
        equal(label + " queueOffset", offset, result.getQueueOffset());
        require(result.getMsgId() != null && !result.getMsgId().isEmpty(), label + ": missing msgId");
        require(result.getOffsetMsgId() != null && !result.getOffsetMsgId().isEmpty(), label + ": missing offsetMsgId");
        System.out.println("JAVA_INTEROP_SEND_OK " + label + " " + result);
    }

    private static List<MessageExt> checkPull(String label, PullResult result, int count, long next, long max) {
        require(result != null && result.getPullStatus() == PullStatus.FOUND, label + ": " + result);
        require(result.getMsgFoundList() != null, label + ": missing messages: " + result);
        equal(label + " message count", count, result.getMsgFoundList().size());
        equal(label + " nextBeginOffset", next, result.getNextBeginOffset());
        equal(label + " minOffset", 0, result.getMinOffset());
        equal(label + " maxOffset", max, result.getMaxOffset());
        return result.getMsgFoundList();
    }

    private static void checkNoMatch(String label, PullResult result, long next) {
        require(result != null && result.getPullStatus() == PullStatus.NO_MATCHED_MSG, label + ": " + result);
        require(result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty(), label + ": unexpected messages");
        equal(label + " nextBeginOffset", next, result.getNextBeginOffset());
        equal(label + " maxOffset", next, result.getMaxOffset());
    }

    private static void checkMessage(String label, MessageExt message, MessageQueue queue, long offset,
        String tag, byte[] body) {
        require(message != null, label + ": null message");
        require(queue.getTopic().equals(message.getTopic()), label + ": wrong topic " + message.getTopic());
        equal(label + " queueId", queue.getQueueId(), message.getQueueId());
        equal(label + " queueOffset", offset, message.getQueueOffset());
        require(tag.equals(message.getTags()), label + ": wrong tag " + message.getTags());
        require(KEY.equals(message.getKeys()), label + ": wrong key " + message.getKeys());
        require(Arrays.equals(body, message.getBody()), label + ": body mismatch; expected " + body.length
            + " bytes, received " + (message.getBody() == null ? "null" : message.getBody().length));
        require(message.getMsgId() != null && !message.getMsgId().isEmpty(), label + ": missing msgId");
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void equal(String label, long expected, long actual) {
        require(expected == actual, label + ": expected=" + expected + ", actual=" + actual);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("JAVA_INTEROP_FAILED: " + message);
        }
    }
}
