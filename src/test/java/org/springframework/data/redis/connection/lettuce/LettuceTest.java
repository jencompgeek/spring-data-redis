package org.springframework.data.redis.connection.lettuce;

import java.util.List;
import java.util.concurrent.Future;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.protocol.Command;

public class LettuceTest {

	@Test
	public void testWatch() throws Exception {
		RedisClient client = new RedisClient("localhost", 6379);
		RedisAsyncConnection<byte[], byte[]> conn1 = client.connectAsync(LettuceUtils.CODEC);
		RedisAsyncConnection<byte[], byte[]> conn2 = client.connectAsync(LettuceUtils.CODEC);
		RedisConnection<byte[],byte[]> syncConn = new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(conn2);
		
		// Watch with conn 1
		Future<String> watch = conn1.watch("testitnow".getBytes());
		
		// Synchronously set the value with conn 2
		syncConn.set("testitnow".getBytes(), "something".getBytes());
		
		// Start conn 1 tx
		Future<String> mul = conn1.multi();
		
		// Attempt to change watched variable value
		Future<String> set = conn1.set("testitnow".getBytes(), "somethingelse".getBytes());

		// Exec tx
		Future<List<Object>> f = conn1.exec();
		List<Object> results = f.get();
		
		// Results should be empty since watched var modified by other connection
		System.out.println(results);
		assertTrue(results.isEmpty());
		
		Command<?,?,?>[] ppline = new Command<?,?,?>[] { (Command<?,?,?>)watch, (Command<?,?,?>)mul, (Command<?,?,?>)set};
		conn1.awaitAll(ppline);
		for (Command<?, ?, ?> cmd : ppline) {
			if (cmd.getOutput().hasError()) {
				// Processing the "set" future often results in "ERR EXEC without MULTI" here
				throw new RuntimeException(cmd.getOutput().getError());
			}else {
				System.out.println(cmd.getOutput().get());
			}
		}
	}
}
