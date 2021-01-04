package org.apache.nifi.processors.tcp.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

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
  	private final String customText;
  	private final int readSecondsTimeout;
  	private final int receiveBufferSize;
  	private final Relationship REL_SUCCESS;
  	private final ProcessSessionFactory sessionFactory;
  	private final SSLContextService sslContextService;
  	private final MessageHandler delegatingMessageHandler;
  	private volatile SSLSocket sslSocket;
  	private Thread threadClient; 
  	
  	public SendingReceivingClient(String host, int port, String customText, int readSecondsTimeout, int receiveBufferSize,
  			Relationship REL_SUCCESS, ProcessSessionFactory sessionFactory, SSLContextService sslContextService, MessageHandler delegatingMessageHandler) {
  		this.host = host;
  		this.port = port;
  		this.customText = customText;
  		this.readSecondsTimeout = readSecondsTimeout;
  		this.receiveBufferSize = receiveBufferSize;
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
            } else {
            	sslContext = SSLContext.getDefault();
            }
            
            this.sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(this.host, this.port);   
            this.sslSocket.setNeedClientAuth(false);
            if (this.readSecondsTimeout > 0) {
                this.sslSocket.setSoTimeout(this.readSecondsTimeout * 1000);
            }

            
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

            bufferedOutputStream.write((this.customText.toString()+"\r\n").getBytes());
            bufferedOutputStream.flush();
            
			List<Byte> buffer = new ArrayList<Byte>();
			while (!this.sslSocket.isClosed()) {
				try {
					int lastByte = bufferedInputStream.read();
					buffer.add((byte)lastByte);
					if (lastByte < 0) {
						sslSocket.close();
						return;
					}
					
					if (buffer.size() >= receiveBufferSize) {
	                    buffer.clear();
					}
					List<Byte>packet = this.delegatingMessageHandler.popPacketFromBuffer(buffer);
					if (packet != null){
						Byte[] packetArray = new Byte[packet.size()];
						packet.toArray(packetArray);
						byte[] packetArrayByte = new byte[packet.size()];
						for(int i = 0; i<packet.size(); i++) {
							packetArrayByte[i] = packetArray[i].byteValue();
						}
	                    this.handle(packetArrayByte);
	                    buffer.clear();
					}
					
				} catch (SocketTimeoutException e) {
			           logger.error("Socket timed out", e);
			           this.stop();
			           return;
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
    
    public void handle(byte [] packet) {
        ProcessSession session = this.sessionFactory.createSession();
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(packet);
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }
  
}
