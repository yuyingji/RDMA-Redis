import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Jedis;

public class AppServer {

    static Map<String, String> p2Configuration;
    static Map<String, JedisClient> connectionPool;
    static Map<String, String> dataToP2;


    public static String processRequest(DummyRequest dq) {

            // hashfunction :x =  n % 3
        String command = dq.command;
        String p2Index = dataToP2.get(String.valueOf(Integer.parseInt(dq.value) % 3));
        JedisClient client = connectionPool.get(p2Index);
        String result = null;
        if (client != null && client.isAvailable) {
            client.isAvailable = false;
            Jedis connection = client.jedis;
            if (command.equals("get")) {
                result = connection.get(dq.value);
            } else if (command.equals("set")) {
                // TODO:
            }
            client.isAvailable = true;
        } else if (client != null && !client.isAvailable){
            while (true) {
                if (client.isAvailable) {
                    client.isAvailable = false;
                    Jedis connection = client.jedis;
                    if (command.equals("get")) {
                        result = connection.get(dq.value);
                    } else if (command.equals("set")) {
                        // TODO:
                    }
                    client.isAvailable = true;
                    break;
                }
            }
        } else { // client not exists
            System.out.println(p2Index);
            String[] hostAndPort = p2Configuration.get(p2Index).split(",");
            String host = hostAndPort[0];
            String port = hostAndPort[1];
            JedisClient newClient = new JedisClient(new Jedis(host, Integer.parseInt(port)), false);
            connectionPool.put(p2Index, newClient);

            Jedis connection = newClient.jedis;
            if (command.equals("get")) {
                result = connection.get(dq.value);
            } else if (command.equals("set")) {
                // TODO:
            }
            newClient.isAvailable = true;
        }

        return result;
    }



    public static void main(String[] args) {
        p2Configuration = new HashMap<String, String>(){{
            put("0", "localhost,8080");
            put("1", "localhost,8080");
            put("2", "localhost,8080");
        }};
        connectionPool = new HashMap<>();
        dataToP2 = new HashMap<String, String>(){{
            put("0", "0");
            put("1", "1");
            put("2", "2");
        }};

        DummyRequest dq1 = new DummyRequest("1", "get");
//        DummyRequest dq2 = new DummyRequest("2", "set");
//        DummyRequest dq3 = new DummyRequest("3", "set");
//        DummyRequest dq4 = new DummyRequest("4", "set");
//        DummyRequest dq5 = new DummyRequest("5", "set");
//        DummyRequest dq6 = new DummyRequest("6", "get");

        DummyRequest[] requests = {dq1};

        for (DummyRequest dq : requests) {
            System.out.println(processRequest(dq));
        }

    }


}

class DummyRequest {

    public String value;
    public String command;

    public DummyRequest(String value, String command) {
        this.value = value;
        this.command = command;
    }

}

class JedisClient {

    public Jedis jedis;
    public boolean isAvailable;
    public JedisClient(Jedis jedis, boolean isAvailable) {
        this.jedis = jedis;
        this.isAvailable = isAvailable;
    }
}
