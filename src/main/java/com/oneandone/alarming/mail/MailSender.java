package com.oneandone.alarming.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class MailSender {

	static Logger logger = LoggerFactory.getLogger(MailSender.class);
	
    public static void sendMail(String mailAddressList, String mailSubject, String smtpHost, String smtpPort, String mailFrom, String pathToMailTemplate, Properties mailContentProperties) {
        if(mailAddressList != null && !mailAddressList.isEmpty()) {
            try
            {
            	String mailText = MailUtils.transformEmailTemplate(pathToMailTemplate, mailContentProperties);

            	Properties p = new Properties();
	            p.put("mail.transport.protocol","smtp");
	            p.put("mail.smtp.host",smtpHost);
	            p.put("mail.smtp.port",smtpPort);
	            Session session = Session.getInstance(p);

                for (String receiver: mailAddressList.split(",")) {
					MimeMessage message = new MimeMessage(session);
					message.setFrom(new InternetAddress(mailFrom));
					message.addRecipient(Message.RecipientType.TO, new InternetAddress(receiver));
					message.setSubject(mailSubject);
					message.setContent(mailText,"text/plain");
					Transport.send(message);
					logger.info("E-mail successfully sent to " + receiver);
                }
            }
            catch (Exception e){
                logger.error(e.getMessage(), e);
            }
        }
    }
	
}
