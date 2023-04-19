package com.jfcbxp.redisson;

import com.jfcbxp.redisson.config.RedissonConfig;
import com.jfcbxp.redisson.dto.PriorityQueue;
import com.jfcbxp.redisson.dto.Student;
import com.jfcbxp.redisson.dto.UserOrder;
import com.jfcbxp.redisson.enums.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.BatchOptions;
import org.redisson.api.DeletedObjectListener;
import org.redisson.api.ExpiredObjectListener;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RBatchReactive;
import org.redisson.api.RBlockingDequeReactive;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RDequeReactive;
import org.redisson.api.RHyperLogLogReactive;
import org.redisson.api.RListReactive;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RMapCacheReactive;
import org.redisson.api.RMapReactive;
import org.redisson.api.RPatternTopicReactive;
import org.redisson.api.RQueueReactive;
import org.redisson.api.RScoredSortedSetReactive;
import org.redisson.api.RSetReactive;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RTransactionReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.TransactionOptions;
import org.redisson.api.listener.PatternMessageListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static jodd.util.ThreadUtil.sleep;

@SpringBootTest
class RedissonApplicationTests {

	private RedissonConfig redissonConfig = new RedissonConfig();
	private RedissonReactiveClient client;
	private RLocalCachedMap<Integer,Student> studentsMap;
	private RBlockingDequeReactive<Long> msgQueue;
	private RBucketReactive<Long> user1Balance;
	private RBucketReactive<Long> user2Balance;

	private PriorityQueue priorityQueue;

	@BeforeEach
	public void setClient(){
		RedissonConfig config = new RedissonConfig();
		var reddisonClient = config.getClient();

		LocalCachedMapOptions<Integer,Student> mapOptions =
				LocalCachedMapOptions.<Integer,Student>defaults().syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE)
						.reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.NONE);

		studentsMap =
				reddisonClient.getLocalCachedMap("students", new TypedJsonJacksonCodec(Integer.class,Student.class),
						mapOptions);
		this.client = this.redissonConfig.getReactiveClient();
		setupQueue();
		setBalance();
	}

	public void setupQueue(){
		msgQueue =  this.client.getBlockingDeque("message-queue",LongCodec.INSTANCE);

		RScoredSortedSetReactive<UserOrder> sortedSet =  this.client.getScoredSortedSet("user:order:queue",new TypedJsonJacksonCodec(UserOrder.class));
		priorityQueue = new PriorityQueue(sortedSet);
	}

	public void setBalance(){
		user1Balance = this.client.getBucket("user:1:balance", LongCodec.INSTANCE);
		user2Balance = this.client.getBucket("user:2:balance", LongCodec.INSTANCE);
		Mono<Void> mono = user1Balance.set(100L)
				.then(user2Balance.set(0L))
				.then();

		StepVerifier.create(mono).verifyComplete();
	}

	@AfterEach
	public void shutdown() {
		this.client.shutdown();
	}

	@Test
	void main() {
		RedissonApplication.main(new String[]{});
	}

	@Test
	void keyValueAccessTest() {
		RBucketReactive<String> bucketReactive = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set("sam");
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();
		StepVerifier.create(set.concatWith(get)).verifyComplete();
	}

	@Test
	void keyValueAccessExpireTest() {
		RBucketReactive<String> bucketReactive = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set("sam", 10, TimeUnit.SECONDS);
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();
		StepVerifier.create(set.concatWith(get)).verifyComplete();
	}

	@Test
	void keyValueAccessExpireExtendTest() {
		RBucketReactive<String> bucketReactive = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set("sam", 10, TimeUnit.SECONDS);
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();
		 StepVerifier.create(set.concatWith(get)).verifyComplete();
		Mono<Boolean> mono =bucketReactive.expire(60,TimeUnit.SECONDS);
		StepVerifier.create(mono).expectNext(true).verifyComplete();
		Mono<Void> ttl = bucketReactive.remainTimeToLive().doOnNext(System.out::println).then();
		StepVerifier.create(ttl).verifyComplete();

	}

	@Test
	void keyValueObjectTest() {
		Student student = new Student("marshal",10,"atlanta", Arrays.asList(1,2,3));
		RBucketReactive<Student> bucketReactive = this.client.getBucket("student:1", new TypedJsonJacksonCodec(Student.class));
		//RBucketReactive<Student> bucketReactive = this.client.getBucket("student:1", JsonJacksonCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set(student);
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();
		StepVerifier.create(set.concatWith(get)).verifyComplete();
	}

	@Test
	void keyValueIncreaseTest() {
		var atomicLong = this.client.getAtomicLong("user:1:visit");
		var mono = Flux.range(1,30).delayElements(Duration.ofSeconds(30))
				.flatMap(i -> atomicLong.incrementAndGet()).then();

		StepVerifier.create(mono).verifyComplete();
	}

	@Test
	void bucketAsMapTest() {
		var mono = this.client.getBuckets(StringCodec.INSTANCE).get("user:1:name","user:2:name")
				.doOnNext(System.out::println).then();
		StepVerifier.create(mono).verifyComplete();
	}

	@Test
	void expiredEventTest() {
		RBucketReactive<String> bucketReactive = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set("sam", 10, TimeUnit.SECONDS);
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();

		var event = bucketReactive.addListener(new ExpiredObjectListener() {
			@Override
			public void onExpired(String s) {
				System.out.println("expired:" + s);
			}
		}).then();

		StepVerifier.create(set.concatWith(get).concatWith(event)).verifyComplete();

		sleep(11000);

	}

	@Test
	void deletedEventTest() {
		RBucketReactive<String> bucketReactive = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
		Mono<Void> set = bucketReactive.set("sam");
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();

		var event = bucketReactive.addListener(new DeletedObjectListener() {
			@Override
			public void onDeleted(String s) {
				System.out.println("deleted:" + s);
			}
		}).then();

		StepVerifier.create(set.concatWith(get).concatWith(event)).verifyComplete();

		sleep(11000);

	}

	@Test
	void mapTest() {
		RMapReactive<String,String> map = this.client.getMap("user:1", StringCodec.INSTANCE);
		var name = map.put("name","jor");
		var age = map.put("age","19");
		var city = map.put("city","belem");
		StepVerifier.create(name.concatWith(age).concatWith(city).then()).verifyComplete();
	}

	@Test
	void mapTest2() {
		RMapReactive<String,String> map = this.client.getMap("user:2", StringCodec.INSTANCE);
		Map<String,String> javaMap = Map.of("name","jor","age","30","city","miami");
		StepVerifier.create(map.putAll(javaMap).then()).verifyComplete();
	}

	@Test
	void mapTest3() {
		var codec = new TypedJsonJacksonCodec(Integer.class, Student.class);
		RMapReactive<Integer,Student> map = this.client.getMap("users", codec);
		Student student = new Student("marshal",10,"atlanta", Arrays.asList(1,2,3));
		Student student2 = new Student("marshal2",10,"atlanta2", Arrays.asList(1,2,3));

		StepVerifier.create(map.put(1,student).concatWith(map.put(2,student2)).then()).verifyComplete();
	}

	@Test
	void mapCacheTest() {
		var codec = new TypedJsonJacksonCodec(Integer.class, Student.class);
		RMapCacheReactive<Integer,Student> mapCache = this.client.getMapCache("users:cache",codec);
		Student student = new Student("marshal",10,"atlanta", Arrays.asList(1,2,3));
		Student student2 = new Student("marshal2",10,"atlanta2", Arrays.asList(1,2,3));

		Mono<Student> st1 = mapCache.put(1,student,5,TimeUnit.SECONDS);
		Mono<Student> st2 = mapCache.put(2,student2,10,TimeUnit.SECONDS);

		StepVerifier.create(st1.then(st2).then()).verifyComplete();

		sleep(4000);

		mapCache.get(1).doOnNext(System.out::println).subscribe();
		mapCache.get(2).doOnNext(System.out::println).subscribe();

		sleep(4000);

		mapCache.get(1).doOnNext(System.out::println).subscribe();
		mapCache.get(2).doOnNext(System.out::println).subscribe();

		sleep(4000);

		mapCache.get(1).doOnNext(System.out::println).subscribe();
		mapCache.get(2).doOnNext(System.out::println).subscribe();

	}

	@Test
	void appServer1Test() {
		Student student = new Student("marshal", 10, "atlanta", Arrays.asList(1, 2, 3));
		Student student2 = new Student("marshal2", 10, "atlanta2", Arrays.asList(1, 2, 3));

		this.studentsMap.put(1,student);
		this.studentsMap.put(2,student2);

		Flux.interval(Duration.ofSeconds(1)).doOnNext(i -> System.out.println(i + "--->" + studentsMap.get(1)))
				.subscribe();

		sleep(6000000);

	}

	@Test
	void appServer2Test() {
		Student student = new Student("marshal-update", 10, "atlanta", Arrays.asList(1, 2, 3));

		this.studentsMap.put(1,student);

	}

	@Test
	void listTest() {
		RListReactive<Long> listReactive = this.client.getList("number-input", LongCodec.INSTANCE);

		List<Long> list = LongStream.rangeClosed(1,10)
				.boxed()
				.toList();

		StepVerifier.create(listReactive.addAll(list).then()).verifyComplete();
		StepVerifier.create(listReactive.size()).expectNext(10)
				.verifyComplete();

	}

	@Test
	void queueTest() {
		RQueueReactive<Long> queue = this.client.getQueue("number-input", LongCodec.INSTANCE);
		Mono<Void> mono =  queue.poll()
				.repeat(3)
				.doOnNext(System.out::println)
				.then(); //remove from begining
		StepVerifier.create(mono)
				.verifyComplete();
		StepVerifier.create(queue.size())
				.expectNext(6)
				.verifyComplete();


	}
	@Test
	void stackTest() {
		RDequeReactive<Long> deque = this.client.getDeque("number-input", LongCodec.INSTANCE);
		Mono<Void> mono =  deque.pollLast()
				.repeat(3)
				.doOnNext(System.out::println)
				.then(); //remove from begining
		StepVerifier.create(mono)
				.verifyComplete();
		StepVerifier.create(deque.size())
				.expectNext(6)
				.verifyComplete();

	}

	@Test
	void consumer1Test() {
		this.msgQueue.takeElements()
				.doOnNext(i -> System.out.println("Consumer 1 "+i))
				.doOnError(System.out::println)
				.subscribe();

		sleep(600_000);
	}

	@Test
	void consumer2Test() {
		this.msgQueue.takeElements()
				.doOnNext(i -> System.out.println("Consumer 2 "+i))
				.doOnError(System.out::println)
				.subscribe();

		sleep(600_000);
	}

	@Test
	void producerTest() {
		Mono<Void> mono = Flux.range(1,1000)
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(i -> System.out.println("Producer " + i))
				.flatMap(i -> this.msgQueue.add(Long.valueOf(i)))
				.then();

		StepVerifier.create(mono)
				.verifyComplete();
	}

	@Test
	void HyperLogLogTest() {
		RHyperLogLogReactive<Long> hyperLogLog = this.client.getHyperLogLog("user:visits", LongCodec.INSTANCE);

		List<Long> collect = LongStream.rangeClosed(1, 25)
				.boxed()
				.collect(Collectors.toList());

		List<Long> collect2 = LongStream.rangeClosed(1, 25)
				.boxed()
				.collect(Collectors.toList());

		Mono<Void> mono = Flux.just(collect, collect2).flatMap(hyperLogLog::addAll).then();

		StepVerifier.create(mono)
				.verifyComplete();

		hyperLogLog.count()
				.doOnNext(System.out::println)
				.subscribe();
	}

	@Test
	void subscriber1Test() {
		RTopicReactive topic = this.client.getTopic("slack-room", StringCodec.INSTANCE);
		topic.getMessages(String.class)
						.doOnError(System.out::println)
						.doOnNext(System.out::println)
						.subscribe();

		sleep(600_000);
	}

	@Test
	void subscriber2Test() {
		RPatternTopicReactive patternTopic = this.client.getPatternTopic("slack-room", StringCodec.INSTANCE);
		patternTopic.addListener(String.class, new PatternMessageListener<String>() {
			@Override
			public void onMessage(CharSequence pattern, CharSequence topic, String message) {
				System.out.println(pattern  + ":" + topic + ":" + message);
			}
		}).subscribe();

		sleep(600_000);
	}

	@Test
	void batchTest() {
		RBatchReactive batch = this.client.createBatch(BatchOptions.defaults());
		RListReactive<Long> list = batch.getList("numbers-list", LongCodec.INSTANCE);
		RSetReactive<Long> set = batch.getSet("numbers-set", LongCodec.INSTANCE);

		list.addAll(LongStream.rangeClosed(0,99)
				.boxed()
				.toList());
		set.addAll(LongStream.rangeClosed(100,200)
				.boxed()
				.toList());
		StepVerifier.create(batch.execute().then())
				.verifyComplete();
	}

	@Test
	void nonTransactionTest(){
		transfer(user1Balance,user2Balance,50)
				.thenReturn(0)
				.map(i -> (5/ i)) //some error
				.subscribe();
		sleep(1000);

	}

	@Test
	void transactionTest(){
		RTransactionReactive transaction = this.client.createTransaction(TransactionOptions.defaults());
		RBucketReactive<Long> userBalance1 = transaction.getBucket("user:1:balance", LongCodec.INSTANCE);
		RBucketReactive<Long> userBalance2 = transaction.getBucket("user:2:balance", LongCodec.INSTANCE);
		transfer(userBalance1,userBalance2,50)
				.thenReturn(0)
				.map(i -> (5/ i)) //some error
				.then(transaction.commit())
				.onErrorResume(ex -> transaction.rollback())
				.subscribe();
		sleep(1000);

	}

	@Test
	void sortedTest(){
		RScoredSortedSetReactive<String> scoredSortedSet = this.client.getScoredSortedSet("student:score", StringCodec.INSTANCE);
		Mono<Void> mono = scoredSortedSet.addScore("sam", 12.25).then(scoredSortedSet.add(23.5, "mike"))
				.then(scoredSortedSet.addScore("jake", 7)).then();
		StepVerifier.create(mono)
				.verifyComplete();

		scoredSortedSet.entryRange(0,1)
				.flatMapIterable(Function.identity())
				.map(se -> se.getScore() + " : "+ se.getValue() )
				.doOnNext(System.out::println)
				.subscribe();

	}

	@Test
	void producerPriorityTest(){
		UserOrder u1 = new UserOrder(1, Category.GUEST);
		UserOrder u2 = new UserOrder(2, Category.STD);
		UserOrder u3 = new UserOrder(3, Category.PRIME);
		UserOrder u4 = new UserOrder(4, Category.GUEST);

		Mono<Void> mono = Flux.just(u1, u2, u3, u4).flatMap(this.priorityQueue::add).then();
		StepVerifier.create(mono)
				.verifyComplete();

	}

	@Test
	void consumerPriorityTest(){
		this.priorityQueue.takeItems()
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(System.out::println)
				.subscribe();

		sleep(600_000);


	}

	Mono<Void> transfer(RBucketReactive<Long>  fromAccount, RBucketReactive<Long> toAccount, int amount){
		return Flux.zip(fromAccount.get(),toAccount.get())
				.filter(t -> t.getT1() >= amount)
				.flatMap(t -> fromAccount.set(t.getT1() - amount).thenReturn(t))
				.flatMap(t -> toAccount.set(t.getT2() + amount)).then();
	}

}
