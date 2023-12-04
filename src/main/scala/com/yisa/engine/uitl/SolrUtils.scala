package com.yisa.engine.uitl

import java.lang.reflect.Field;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.LoggerFactory;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

/**
 * solr 工具类
 * Created by BackZero on 2016/11/29 0029.
 */
object SolrUtils extends Serializable {

  /**
   * solr 服务器访问地址
   */
//  val url: String = "http://27.10.20.6:18983/solr/vehicle";

  val connectionTimeout: Integer = 5000; // socket read timeout  

  val defaltMaxConnectionsPerHost: Integer = 10;

  val maxTotalConnections: Integer = 100;

  val followRedirects: Boolean = false; // defaults to false  
  val allowCompression: Boolean = true;

  // private static  Integer maxRetries = 1 ; //defaults to 0.  > 1 not recommended.  

  /**
   * 建立solr链接，获取 HttpSolrClient
   * @return HttpSolrClient
   */
  def connect(url: String): HttpSolrClient = {
    val httpSolrClient: HttpSolrClient = new HttpSolrClient.Builder(url).build();
    httpSolrClient.setParser(new XMLResponseParser()); //设定xml文档解析器  
    httpSolrClient.setConnectionTimeout(connectionTimeout); //socket read timeout  
    httpSolrClient.setAllowCompression(allowCompression);
    httpSolrClient.setMaxTotalConnections(maxTotalConnections);
    httpSolrClient.setDefaultMaxConnectionsPerHost(defaltMaxConnectionsPerHost);
    httpSolrClient.setFollowRedirects(followRedirects);

    return httpSolrClient;
  }

}  