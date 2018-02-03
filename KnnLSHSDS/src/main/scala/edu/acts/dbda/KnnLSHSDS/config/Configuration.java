package edu.acts.dbda.KnnLSHSDS.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

public class Configuration {
  
  private static Logger logger = Logger.getLogger(Configuration.class);

  final static String KEY_CONFIG_FILE = "CONF_FILE";
  
  /*
   * Spark master URL info
   */
  public static String FLUME_AGENT_HOST;
  public static final String KEY_FLUME_AGENT_HOST = "FLUME_AGENT_HOST";  

  /*
   * Spark master URL info
   */
  public static int FLUME_AGENT_PORT;
  public static final String KEY_FLUME_AGENT_PORT = "FLUME_AGENT_PORT";  
  
  /*
   * Zookeeper info
   */
  public static String TRAIN_DATA;
  public static final String KEY_TRAIN_DATA = "TRAIN_DATA";
  


  /*
   * Kafka broker details along with port
   */
  public static int BATCH_SIZE; 
  public static final String KEY_BATCH_SIZE = "BATCH_SIZE";
  
  /*
   *Kafka group id 
   */
  public static String RESULT_DIR;
  public static final String KEY_RESULT_DIR = "RESULT_DIR";
  
  
  
  static boolean loadProperties(Properties incomingProps) {

    // logger.info(incomingProps);
    
	  FLUME_AGENT_HOST = incomingProps.getProperty(KEY_FLUME_AGENT_HOST, "slave1");
	  FLUME_AGENT_PORT = Integer.parseInt(incomingProps.getProperty(KEY_FLUME_AGENT_PORT, "9999"));
	  TRAIN_DATA = incomingProps.getProperty(KEY_TRAIN_DATA, "/data/ECG200_TRAIN");
	  BATCH_SIZE = Integer.parseInt(incomingProps.getProperty(KEY_BATCH_SIZE, "1"));
	  RESULT_DIR = incomingProps.getProperty(KEY_RESULT_DIR, "/output/Predication");
    
    
    
    logger.info("Using properties as below");
    logger.info("FLUME_AGENT_HOST: " + FLUME_AGENT_HOST);
    logger.info("FLUME_AGENT_PORT: " + FLUME_AGENT_PORT);
    logger.info("TRAIN_DATA: " + TRAIN_DATA);
    logger.info("BATCH_SIZE: " + BATCH_SIZE);
    logger.info("RESULT_DIR: " + RESULT_DIR);
   
    
    return true;
  }
  
  static {
    try {
      Properties defaultProps = new Properties();
      // possible options for CONF_FILE = conf/dev.properties OR conf/test.properties OR absolute path
      
      String configFile = System.getProperty(KEY_CONFIG_FILE);
      
      if( configFile == null ) {
        logger.info("Found KEY_CONFIG_FILE as null using config file at conf/dev.properties");
        configFile = "conf/dev.properties";
      }else {
        logger.info("Using config file from : " + configFile);
      }
      
      FileInputStream in = new FileInputStream(configFile);
      defaultProps.load(in);
      in.close();
      
      loadProperties(defaultProps);
     
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unused")
public static void main(String[] args) {
    Configuration config = new Configuration();
    
    // TEST

    logger.info("Using properties as below");
    logger.info("FLUME_AGENT_HOST: " + FLUME_AGENT_HOST);
    logger.info("FLUME_AGENT_PORT: " + FLUME_AGENT_PORT);
    logger.info("TRAIN_DATA: " + TRAIN_DATA);
    logger.info("BATCH_SIZE: " + BATCH_SIZE);
    logger.info("RESULT_DIR: " + RESULT_DIR);
   
  }
  
}
