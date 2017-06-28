package org.cubedb.api.ext;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Config;
import com.jsoniter.spi.TypeLiteral;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Created by krash on 28.06.17.
 */
@Provider
@Consumes({MediaType.APPLICATION_JSON, "text/json", "application/*+json"})
@Produces({MediaType.APPLICATION_JSON, "text/json", "application/*+json"})
public class JsonIteratorConverter implements MessageBodyReader<Object>, MessageBodyWriter<Object> {
    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public Object readFrom(Class<Object> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream) throws IOException, WebApplicationException {
        JsonIterator smth = JsonIterator.parse(inputStream, 2048);
        Object retval = smth.read();
        return retval;
    }

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public long getSize(Object o, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Object object, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream) throws IOException, WebApplicationException {
        JsonStream.serialize(new Config.Builder().build(), object, outputStream);
    }
}
