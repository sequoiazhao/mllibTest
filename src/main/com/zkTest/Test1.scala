package zkTest

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.{CreateMode, WatchedEvent, ZooDefs}

/**
  * @author zhaoming on 2018-11-08 10:45
  **/
object Test1 {

  def main(args: Array[String]): Unit = {

    val client = CuratorFrameworkFactory.newClient("192.168.99.100:2181", new RetryNTimes(10, 5000))
    client.start()

    val children = client.getChildren.usingWatcher(new CuratorWatcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        println("监控" + watchedEvent)
      }
    }).forPath("/")

    children.toArray.foreach(println)

    val result = client.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/test", "Data".getBytes());
    client.setData.forPath("/test", "111".getBytes)
    client.setData.forPath("/test", "222".getBytes)

    println( client.checkExists().forPath("/test"))



//    client.close()

  }
}
