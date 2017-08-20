//package com.labServer.util;
//
//import java.io.InputStream;
//
//import org.apache.ibatis.session.SqlSession;
//import org.apache.ibatis.session.SqlSessionFactory;
//import org.apache.ibatis.session.SqlSessionFactoryBuilder;
//
//public class MyBatisUtil {
//  // http://www.cnblogs.com/xdp-gacl/p/4262895.html
//	
//  private static SqlSessionFactory factory;
//	
//  /**
//   * 获取SqlSessionFactory
//   * 
//   * @return
//   */
//  static {
//    String resource = "conf.xml";
//    InputStream is = MyBatisUtil.class.getClassLoader().getResourceAsStream(resource);
//    factory = new SqlSessionFactoryBuilder().build(is);
//  }
//
//  public static SqlSession getSqlSession() {
//    return factory.openSession();
//  }
//
//  public static SqlSession getSqlSession(boolean isAutoCommit) {
//    return factory.openSession(isAutoCommit);
//  }
//}
