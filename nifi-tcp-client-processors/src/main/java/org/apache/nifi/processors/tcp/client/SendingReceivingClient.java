package org.apache.nifi.processors.tcp.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.ssl.SSLContextService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendingReceivingClient {
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final String host;
	private final int port;
  	private final String credentials;
  	private final Relationship REL_SUCCESS;
  	private final ProcessSessionFactory sessionFactory;
  	private final SSLContextService sslContextService;
  	private final MessageHandler delegatingMessageHandler;
  	private volatile SSLSocket sslSocket;
  	private Thread threadClient; 
  	
  	
  	public SendingReceivingClient(String host, int port, String credentials, Relationship REL_SUCCESS, ProcessSessionFactory sessionFactory, SSLContextService sslContextService, MessageHandler delegatingMessageHandler) {
  		this.host = host;
  		this.port = port;
  		this.credentials = credentials;
  		this.REL_SUCCESS = REL_SUCCESS;
  		this.sessionFactory = sessionFactory;
  		this.sslContextService = sslContextService;
  		this.delegatingMessageHandler = delegatingMessageHandler;
  	}
  	
  	// connect to server
    private void init() {
        try {
                       
            SSLContext sslContext = null;
            if (this.sslContextService != null) {
                sslContext = this.sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            }
            
            this.sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(this.host, this.port);   
            this.sslSocket.setNeedClientAuth(false);
            
        } catch (Exception e) {
        	logger.error("Socket init error", e);
        }
    }
    
    // process events read/write
    private void process() {
        if (this.sslSocket == null) {
        	logger.error("Socket not initialized");
            return;
        }
        try {
            InputStream inputStream = this.sslSocket.getInputStream();
            OutputStream outputStream = this.sslSocket.getOutputStream();

            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

            bufferedOutputStream.write((this.credentials.toString()+"\r\n").getBytes());
            bufferedOutputStream.flush();
            while (true && !this.sslSocket.isClosed()) {
                byte[] buffer = new byte[40];
                int bytesRead = bufferedInputStream.read(buffer);
                buffer = Arrays.copyOfRange(buffer, 0, bytesRead);
                delegatingMessageHandler.byteArrayInput = ArrayUtils.addAll(delegatingMessageHandler.byteArrayInput, buffer);
                if(bytesRead < 0) {
                	this.stop();
                	return;
                }
                byte[] packet = null;
                while((packet = this.delegatingMessageHandler.popFromByteArray()) != null) {
                	String jsonString = this.delegatingMessageHandler.unGunzipFile(packet); 
                    this.handle(jsonString);
                    
                }
            }
        } catch (IOException e) {
        	logger.error("Socket process error", e);
        } catch (Exception e) {
        	logger.error("Socket process error", e);
        	try {
				this.sslSocket.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
    }
  	
  	public void start() {
        if (this.sslSocket == null || this.sslSocket.isClosed()) {
        	this.init();
        }
        if (this.sslSocket != null && !this.sslSocket.isClosed() && (this.threadClient == null || !this.threadClient.isAlive())) {
      		this.threadClient = new Thread(){
                public void run(){
                    process();
                }
              };
            this.threadClient.start();
        }
  	}
  		

    public void stop() {
		if (this.sslSocket != null && !this.sslSocket.isClosed()) {
			try {
				this.sslSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("Socket not finalized correctly ", e);
			}
		}
    }
    
    public void handle(String message) {
        ProcessSession session = this.sessionFactory.createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(message.getBytes());
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }
    



}
