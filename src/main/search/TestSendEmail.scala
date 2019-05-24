package com.hisense.search.ltr.Common

import java.util.{Date, Properties}
import javax.mail._
import javax.mail.internet.{AddressException, InternetAddress, MimeMessage}

/**
  * @author zhaoming on 2018-09-17 14:35
  **/
object TestSendEmail {

  def sendErrorMail(className: String): Unit = {
    val properties = new Properties()
    properties.put("mail.transport.protocol", "smtp")
    properties.put("mail.smtp.host", "smtp.163.com") // smtp.gmail.com?
    properties.put("mail.smtp.port", "25")
    properties.put("mail.smtp.auth", "true")
    val username = "monitoringzm"
    val password = "hisensezm"


    val authenticator = new Authenticator() {
      override def getPasswordAuthentication: PasswordAuthentication = new
          PasswordAuthentication(username, password)
    }


    val session = Session.getDefaultInstance(properties, authenticator)
    val message = new MimeMessage(session)


    try {

      val from = new InternetAddress("monitoringzm@163.com")
      message.setFrom(from)
      val to = new InternetAddress("zhaoming7@hisense.com")
      message.setRecipient(Message.RecipientType.TO, to)

      val cc = new InternetAddress("monitoringzm@163.com")
      message.addRecipient(Message.RecipientType.CC ,cc)

      message.setSubject("报错信息：airflow"+className)
      val content = "数据表结果为空，code data is null"
      message.setContent(content, "text/html;charset=UTF-8")

      message.setSentDate(new Date())
      message.saveChanges()

      val trans = session.getTransport("smtp")

      trans.connect("smtp.163.com", "monitoringzm@163.com", "123456qw")
      trans.sendMessage(message, message.getAllRecipients)
      trans.close()
      println("success")
    } catch {
      case ex: AddressException => ex.printStackTrace()
      case ex: MessagingException => ex.printStackTrace()
    }

  }

}
