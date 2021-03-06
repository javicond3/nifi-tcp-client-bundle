/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.tcp.client;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.SSLContextService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Tags({"get", "tcp", "stream", "tls", "ssl"})
@CapabilityDescription("Connects over TCP/TLS to the provided endpoint. Received data will be ungzipped and written as content to the FlowFile")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "10 sec")
public class TCPClient extends AbstractSessionFactoryProcessor {
	
	
	
	private String host;
	private int port;
  	private String customText;
	private int stx;	// start byte
  	private int etx;	// end byte
	private int dle;	// escape byte
	private int receiveBufferSize;
	private int readSecondsTimeout;
  	private SSLContextService sslContextService;
  	private SendingReceivingClient client;
	
    private final static List<PropertyDescriptor> DESCRIPTORS;

    private final static Set<Relationship> RELATIONSHIPS;

    
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The ip address or hostname of the destination.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port on the destination.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CUSTOM_TEXT = new PropertyDescriptor.Builder()
            .displayName("Custom Text")
            .name("generate-custom-text")
            .description("Custom text to send when connection is established")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor START_OF_MESSAGE_BYTE = new PropertyDescriptor.Builder()
            .name("start-of-message-byte")
            .displayName("Start of message delimiter byte")
            .description("Byte value which denotes start of message. Must be specified as integer within "
                    + "the valid byte range (-128 thru 127). For example, '2' = Start of message.")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.createLongValidator(-128, 127, true))
            .build();
    
    public static final PropertyDescriptor END_OF_MESSAGE_BYTE = new PropertyDescriptor.Builder()
            .name("end-of-message-byte")
            .displayName("End of message delimiter byte")
            .description("Byte value which denotes end of message. Must be specified as integer within "
                    + "the valid byte range (-128 thru 127). For example, '3' = End of text.")
            .required(true)
            .defaultValue("3")
            .addValidator(StandardValidators.createLongValidator(-128, 127, true))
            .build();
    public static final PropertyDescriptor ESCAPE_MESSAGE_BYTE = new PropertyDescriptor.Builder()
            .name("escape-message-byte")
            .displayName("Escape message byte")
            .description("Byte value which escape end of message byte and start of message byte . Must be specified as integer within "
                    + "the valid byte range (-128 thru 127). For example, '16' = Data link escape.")
            .required(true)
            .defaultValue("16")
            .addValidator(StandardValidators.createLongValidator(-128, 127, true))
            .build();
    
    public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("receive-buffer-size")
            .displayName("Receive Buffer Size")
            .description("The size of the buffer to receive data in. Default 16384 (16MB).")
            .required(false)
            .defaultValue("16MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SECONDS_OF_INACTIVITY = new PropertyDescriptor.Builder()
            .name("seconds-of-inactivity")
            .displayName("Seconds of inactivity")
            .description("")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.createLongValidator(-128, 127, true))
            .build();
    
    public static final PropertyDescriptor READ_SECONDS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-seconds-tiemout")
            .displayName("Read Seconds Timeout")
            .description("Tiemout in seconds  without receiveing data. If this number is reached, " +
            		"the connection is restablished. If it is 0, it is omitted")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be sent over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the endpoint will be sent to.")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The relationship that all failure messages will be sent to. A possible cause is that a message " +
            		"is greater than the RECEIVE_BUFFER_SIZE.")
            .build();
    

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HOSTNAME);
        _propertyDescriptors.add(PORT);
        _propertyDescriptors.add(CUSTOM_TEXT);
        _propertyDescriptors.add(START_OF_MESSAGE_BYTE);
        _propertyDescriptors.add(END_OF_MESSAGE_BYTE);
        _propertyDescriptors.add(ESCAPE_MESSAGE_BYTE);
        _propertyDescriptors.add(RECEIVE_BUFFER_SIZE);
        _propertyDescriptors.add(READ_SECONDS_TIMEOUT);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);

        DESCRIPTORS = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(_relationships);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }
    
    

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	this.host = context.getProperty(HOSTNAME).getValue();
    	this.port = context.getProperty(PORT).asInteger();
      	this.customText = context.getProperty(CUSTOM_TEXT).getValue();
    	this.stx = context.getProperty(START_OF_MESSAGE_BYTE).asInteger();
      	this.etx = context.getProperty(END_OF_MESSAGE_BYTE).asInteger();
      	this.dle = context.getProperty(ESCAPE_MESSAGE_BYTE).asInteger();
    	this.receiveBufferSize = context.getProperty(RECEIVE_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
    	this.readSecondsTimeout = context.getProperty(READ_SECONDS_TIMEOUT).asInteger();
    	this.sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.client == null) {
        	this.client = new SendingReceivingClient(
        			this.host, this.port, this.customText, this.readSecondsTimeout, this.receiveBufferSize, REL_SUCCESS,
        			sessionFactory, this.sslContextService,
        			new MessageHandler(this.stx, this.etx, this.dle)
        			);
        	
        }
        this.client.start();
        
        
    }
    
    @OnStopped
    public void tearDown() {
    	this.client.stop();
    }
    
    
    

}
