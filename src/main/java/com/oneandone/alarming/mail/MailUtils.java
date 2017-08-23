package com.oneandone.alarming.mail;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

public class MailUtils {

	public static final String EMAIL_TEMPLATE_PROPERTY_START = "#!";
    public static final String EMAIL_TEMPLATE_PROPERTY_END = "#";

    /**
     * Reads an Email template from the specified resource path and transforms the content
     * with the specified property values.
     * @param templatePath resource path of the Email template file
     * @param properties email content properties
     * @return transformed email content
     * @throws IOException throws exception if template can not be read
     */
    public static String transformEmailTemplate(String templatePath, Properties properties) throws IOException {
    	String emailContent = IOUtils.toString(MailUtils.class.getResourceAsStream(templatePath));
    	if (emailContent != null && properties != null && properties.size() > 0) {
    		String templateProperty;
    		for (Object propertyName : properties.keySet()) {
    			templateProperty = EMAIL_TEMPLATE_PROPERTY_START + propertyName + EMAIL_TEMPLATE_PROPERTY_END;
    			emailContent = emailContent.replaceAll(templateProperty, "" + properties.get(propertyName));
    		}
    	}
		return emailContent;
    }

}
