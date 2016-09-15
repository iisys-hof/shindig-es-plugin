package org.apache.shindig.elasticsearch;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Utility class that reads and provides the configuration.
 */
public class ESConfig
{
    private static final String PROPERTIES = "shindig-es-plugin";

    private final Map<String, String> fProperties;
    
    /**
     * Initializes the class by reading the configuration properties file.
     *
     * @throws Exception
     *           if any errors occur
     */
    public ESConfig() throws Exception
    {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final ResourceBundle rb = ResourceBundle.getBundle(PROPERTIES,
                Locale.getDefault(), loader);

        this.fProperties = new HashMap<String, String>();

        String key = null;
        String value = null;
        final Enumeration<String> keys = rb.getKeys();
        while (keys.hasMoreElements())
        {
          key = keys.nextElement();
          value = rb.getString(key);

          this.fProperties.put(key, value);
        }
    }

    /**
     * Creates an empty configuration object, without reading a properties, for testing purposes.
     *
     * @param test
     *          redundant parameter
     */
    public ESConfig(boolean test)
    {
      this.fProperties = new HashMap<String, String>();
    }

    /**
     * Sets the value for a property key.
     *
     * @param key
     *          key of the property
     * @param value
     *          value of the property
     */
    public void setProperty(String key, String value)
    {
      this.fProperties.put(key, value);
    }

    /**
     * Returns the value for a property key or null if it doesn't exist.
     *
     * @param key
     *          key of the property
     * @return value of the property
     */
    public String getProperty(String key)
    {
      return this.fProperties.get(key);
    }
}
