package org.cubedb.api.utils;

import java.util.Map;

import javax.ws.rs.core.UriInfo;

import org.cubedb.utils.CubeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlike.genson.Genson;

public class APIResponse<T> {

	private static final Logger log = LoggerFactory.getLogger(APIResponse.class);
	private static final Genson genson = new Genson();
	public APIResponse() {

	}

	@Override
	public String toString() {
		return "APIResponse [header=" + header + ", response=" + response + "]";
	}

	public APIResponse(T response, UriInfo info, long startTime) {
		this.response = response;
		this.header = new Header(startTime, info);
		
	}

	public static class Header {
		@Override
		public String toString() {
			return "Header [requestTs=" + requestTs + ", processingTime=" + processingTimeMs + "]";
		}

		public Header() {

		}

		public Header(long startTime, UriInfo info) {
			this.requestTs = System.currentTimeMillis();
			this.processingTimeMs = this.requestTs - startTime;
			// There is a bug in Genson not allowing to serialize MultiValuedMaps
			this.request = CubeUtils.multiValuedMapToMap(info.getQueryParameters());
			this.params = CubeUtils.multiValuedMapToMap(info.getPathParameters());
			log.info("{}ms\t{}\t{}\t{}", this.processingTimeMs, info.getAbsolutePath(), genson.serialize(this.request), genson.serialize(this.params));
		}

		public long requestTs;
		public long processingTimeMs;
		public Map< String, String[]> request;
		public Map< String, String[]> params;
	}

	public Header header;
	public T response;
}
