package at.ac.uniak.tsdb;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.meta.Annotation;
import net.opentsdb.tsd.RTPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.lang.Math;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.EventDSL;
import org.jboss.netty.channel.DefaultChannelFuture;

public class RiemannPublisher extends RTPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(RiemannPublisher.class);
  private String  configHosts;
  private String[] riemannHosts;
  private int riemannPort;
  private RiemannClient[] clients;
  private Date lastException;
  //keywords reserve for riemann
  private String[] keywords = {"host","state","ttl","description"}; 
  private EventDSL event;

  public void initialize(final TSDB tsdb) {
    LOG.info("initialize RiemannPublisher");

    riemannPort = tsdb.getConfig().getInt("tsd.plugin.riemann.port");
    configHosts = tsdb.getConfig().getString("tsd.plugin.riemann.host");
    riemannHosts = configHosts.split(",");
    clients = new RiemannClient[riemannHosts.length];

    try {
      /* Set this here cause of a Bug in netty -> deadlock */
      DefaultChannelFuture.setUseDeadLockChecker(false);
      for (int i=0; i< riemannHosts.length; i++)  {
            clients[i] = RiemannClient.tcp(new InetSocketAddress(riemannHosts[i],riemannPort));
            clients[i].connect();
      }
      LOG.info("Successfully connected RiemannClient");
    } catch (IOException e) {
      LOG.error("IOException while trying to create RiemannClient");
    }

  }

  public Deferred<Object> shutdown() {
    return new Deferred<Object>();
  }

  public Deferred<Object> publishAnnotation(final Annotation annotation) {
    return new Deferred<Object>();
  }

  public String version() {
    return "0.0.4";
  }

  public void collectStats(final StatsCollector collector) {
  }
  
  private Collection filter_keywords(final Map<String, String> tags) {
      for (String keyword : keywords)  {
          if (tags.keySet().contains(keyword)) {
		tags.remove(keyword);
          }
      }
      return tags.values();
  }

  public int chooseRiemann(final String service) {
      int hash = 7;
      for (int i=0; i<service.length(); i++) {
          hash = hash * 31  + service.charAt(i);
      }
      return Math.abs(hash%clients.length);
  }

  public Deferred<Object> publishDataPoint(
      final String metric,
      final long timestamp,
      final long value,
      final Map<String, String> tags,
      final byte[] tsuid) {

    try {
      HashSet<String> keys = new HashSet<String>(tags.keySet()); 
      String state = keys.contains("state") ? tags.get("state") : null;
      Float ttl = keys.contains("ttl") ? Float.parseFloat(tags.get("ttl")) : null;
      String hostName = keys.contains("host") ? tags.get("host") : null;
      String description = keys.contains("description") ? tags.get("description") : null;
      List<String> tagValues = new ArrayList<String>(filter_keywords(tags));
      int riemannIndex = chooseRiemann(metric);

      if (clients[riemannIndex].isConnected()) {
	      event = clients[riemannIndex].event();
	      event.host(hostName).
		      service(metric).
		      metric(value);
	      event.tags(tagValues);
	      if (ttl != null) {
		      event.ttl(ttl);
	      }
	      if (state != null) {
		      event.state(state);
	      }
	      if (description != null) {
		      event.description(description);
	      }
	      event.send();
      } else {
        reconnectClient(riemannIndex);
      }
    } catch (Throwable t) {
      LOG.error("Exception in RiemannPublisher publishDataPoint", t);
    }
    return new Deferred<Object>();
  }

  public Deferred<Object> publishDataPoint(
      final String metric,
      final long timestamp,
      final double value,
      final Map<String, String> tags,
      final byte[] tsuid) {

    try {
      HashSet<String> keys = new HashSet<String>(tags.keySet()); 
      String state = keys.contains("state") ? tags.get("state") : null;
      Float ttl = keys.contains("ttl") ? Float.parseFloat(tags.get("ttl")) : null;
      String hostName = keys.contains("host") ? tags.get("host") : null;
      String description = keys.contains("description") ? tags.get("description") : null;
      List<String> tagValues = new ArrayList<String>(filter_keywords(tags));
      int riemannIndex = chooseRiemann(metric);

      if (clients[riemannIndex].isConnected()) {
	      event = clients[riemannIndex].event();
	      event.host(hostName).
		      service(metric).
		      metric(value);
	      event.tags(tagValues);
	      if (ttl != null) {
		      event.ttl(ttl);
	      }
	      if (state != null) {
		      event.state(state);
	      }
	      if (description != null) {
		      event.description(description);
	      }
	      event.send();
      } else {
        reconnectClient(riemannIndex);
      }

    } catch (Throwable t) {
      LOG.error("Exception in RiemannPublisher publishDataPoint", t);
    }
    return new Deferred<Object>();
  }

  private synchronized void reconnectClient(int riemannIndex) {
    if (lastException == null) {
      lastException = new Date();
      return;
    }
    Date currentTime = new Date();
    long timeDiffInMillis = Math.abs(currentTime.getTime() - lastException.getTime());
    if (timeDiffInMillis <= 5*1000) {
      try {
        clients[riemannIndex].reconnect();
      } catch (IOException ioe) {
        LOG.error("Error while trying to reconnect to Riemann Server, will retry in 5 Seconds");
        lastException = new Date();
      }
    }
  }
}
