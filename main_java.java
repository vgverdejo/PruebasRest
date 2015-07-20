// import org.springframework.web.client.RestTemplate;


// public class main_java{

//     public static void main(String[] args) {
//         String result =
//         restTemplate.getForObject(
//         "http://127.0.0.1:5000/words");
//         System.out.println(result);
//     }

// }


package samples.helloworld.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

public class main_java {
    public main_java() {
        clientConfig = new DefaultClientConfig();
    client = Client.create(clientConfig);

    resource = client.resource("http://localhost:8080");
    // lets get the XML as a String
    String text = resource("foo").accept("application/xml").get(String.class);
    }

}
