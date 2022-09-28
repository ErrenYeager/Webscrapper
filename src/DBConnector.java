import com.datastax.driver.core.*;

public class DBConnector {
    private Cluster cluster;
    private Session session;

    public void connectDB(String seeds, int port){
        this.cluster = new Cluster.Builder().addContactPoint(seeds).withQueryOptions(new QueryOptions().setFetchSize(Integer.MAX_VALUE)).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();

        for (final Host host : metadata.getAllHosts()) {
            System.out.println("Driver version " + host.getCassandraVersion());
        }

        this.session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        cluster.close();
    }
}
