package jonathan.mayer.mapdservices;

import com.mapd.thrift.server.MapD;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TTransport;

public class MapDClientUtils {

	public static MapD.Client get_client(String host_or_uri, int port, boolean http) {
		THttpClient httpTransport;
		TTransport transport;
		TBinaryProtocol protocol;
		TJSONProtocol jsonProtocol;
		TSocket socket;
		MapD.Client client;

		try {
			if (http) {
				httpTransport = new THttpClient(host_or_uri);
				jsonProtocol = new TJSONProtocol(httpTransport);
				client = new MapD.Client(jsonProtocol);
				httpTransport.open();
				return client;
			} else {
				transport = new TSocket(host_or_uri, port);
				protocol = new TBinaryProtocol(transport);
				client = new MapD.Client(protocol);
				transport.open();
				return client;
			}
		} catch (TException x) {
			x.printStackTrace();
		}
		return null;
	}
}
