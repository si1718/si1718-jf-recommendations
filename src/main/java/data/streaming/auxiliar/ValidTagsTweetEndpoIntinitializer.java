package data.streaming.auxiliar;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.streaming.connectors.twitter.TwitterSource.EndpointInitializer;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

public class ValidTagsTweetEndpoIntinitializer implements EndpointInitializer, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3172979177938307047L;

	private List<String> tagNames;

	private StatusesFilterEndpoint endpoint;

	public ValidTagsTweetEndpoIntinitializer(List<String> tagNames) {
		super();
		this.tagNames = tagNames;
	}

	public StreamingEndpoint createEndpoint() {
		endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(tagNames);
		return endpoint;

	}

	public List<String> getTagNames() {
		return tagNames;
	}
}
