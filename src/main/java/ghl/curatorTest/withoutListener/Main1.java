package ghl.curatorTest.withoutListener;

public class Main1 {
	public static void main(String[] args) {
		LeaderTest1 test1 = new LeaderTest1("10.96.29.30:8080");
		test1.start();
		LeaderTest1 test2 = new LeaderTest1("10.96.29.31:9090");
		test2.start();
	}
}
