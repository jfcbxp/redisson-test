package com.jfcbxp.redisson;

import com.jfcbxp.redisson.config.RedissonConfig;
import com.jfcbxp.redisson.dto.Student;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RedissonApplicationTests {

	private RedissonConfig redissonConfig = new RedissonConfig();
	private RedissonReactiveClient client;

	@BeforeEach
	public void setClient(){
		this.client = this.redissonConfig.getReactiveClient();
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
		Mono<Void> set = bucketReactive.set(student);
		Mono<Void> get = bucketReactive.get().doOnNext(System.out::println).then();
		StepVerifier.create(set.concatWith(get)).verifyComplete();
	}

}
