package org.example;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        // Register only your application POJOs
        kryo.register(Person.class);

        // IMPORTANT: allow unregistered classes to be handled by Kryo defaults
        // instead of forcing registration (which triggers FieldSerializer on JVM internals).
        kryo.setRegistrationRequired(false);
    }
}
