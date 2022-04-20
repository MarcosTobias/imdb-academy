package co.empathy.academy.search;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.SpringVersion;

@SpringBootTest
class SearchApplicationTests {

	@Test
	void contextLoads() {
		System.out.println("version: " + SpringVersion.getVersion());
	}

}
