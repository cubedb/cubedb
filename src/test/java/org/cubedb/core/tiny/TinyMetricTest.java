package org.cubedb.core.tiny;

import static org.junit.Assert.assertEquals;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.cubedb.core.BaseMetricTest;
import org.junit.Test;

import java.io.ByteArrayInputStream;

/** Created by krash on 28.06.17. */
public class TinyMetricTest extends BaseMetricTest {
  @Override
  protected TinyMetric createMetric() {
    return new TinyMetric();
  }

  @Test
  public void testSerDe() {
    TinyMetric metric = createMetric();
    for (int i = 1; i <= 10; i++) {
      metric.append(i);
    }
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    Output output = new Output(bao);
    kryo.writeObject(output, metric);
    output.close();
    TinyMetric deser =
        kryo.readObject(new Input(new ByteArrayInputStream(bao.toByteArray())), TinyMetric.class);
    assertEquals(metric.size(), deser.size());
    assertEquals(metric.getNumRecords(), deser.getNumRecords());
    for (int i = 0; i < metric.getNumRecords(); i++) {
      assertEquals(metric.get(i), deser.get(i));
    }
  }
}
