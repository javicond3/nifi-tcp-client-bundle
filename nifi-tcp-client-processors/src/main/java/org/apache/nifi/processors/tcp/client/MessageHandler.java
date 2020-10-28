package org.apache.nifi.processors.tcp.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageHandler {
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private int stx;	// start byte
  	private int etx;	// end byte
	private int dle;	// escape byte
	byte[] byteArrayInput;

	MessageHandler(int stx, int etx, int dle) {
        this.stx = stx;
        this.etx = etx;
        this.dle = dle;
    }
	
	// extract packet from byteArrayInput
    public byte[] popFromByteArray(){
    	int EOP = -1;
    	for (int i = 0; i < this.byteArrayInput.length -1; i++) {
    		if(this.byteArrayInput[i] == this.dle) {
    			if(this.byteArrayInput[i+1] == this.dle) {
    				i++;
    			} else if(this.byteArrayInput[i+1] == this.etx) {
    				// limited found, end of packet
    				EOP = i;
    				break;
    			}
    		}
    	}
    	if (EOP != -1) {
    		// entire packet without EOP
    		byte[] packet = Arrays.copyOfRange(this.byteArrayInput, 0, EOP);
    		
    		//pop the content of packet form byteArrayInput
    		if(EOP+2 < this.byteArrayInput.length) {
    			this.byteArrayInput = Arrays.copyOfRange(this.byteArrayInput, EOP+2, this.byteArrayInput.length);
    		} else {
    			this.byteArrayInput = new byte[0];    			
    		}
    		if(packet[0] != this.dle && packet[0] != this.stx) {
    			logger.warn("Popped a packet without a valid start delimiter");
    			return null;
    		}
    		// Delete DLE STX
    		packet = Arrays.copyOfRange(packet, 2, packet.length);
    		packet = this.deStuff(packet);
    		return packet;
    		
    	}
    	
    	return null;
    }
    
    //delete duplicate delimiters from the packet:
    //dle dle to dle
    public byte[] deStuff(byte[] byteArray) {
    	byte[] byteArrayOut = new byte[byteArray.length];
    	int o = 0;
    	for (int i = 0; i < byteArray.length; i++, o++) {
    		if(byteArray[i] == this.dle && i < byteArray.length -1 && byteArray[i+1] == this.dle) {
    			byteArrayOut[o] = byteArray[i];
    			i++;
    		}else {
    			byteArrayOut[o] = byteArray[i]; 
    		}
    	}
    	return Arrays.copyOfRange(byteArrayOut, 0, o);
    }
    
    public String unGunzipFile(byte[] byteArray) {
    	 
        byte[] buffer = new byte[1024];
        byte[] outByteArray = new byte[0];
 
        try {
        	
            GZIPInputStream gZIPInputStream = new GZIPInputStream(new ByteArrayInputStream(byteArray));
            int bytes_read;
 
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
 
            	buffer = Arrays.copyOfRange(buffer, 0, bytes_read);
           	 	outByteArray = ArrayUtils.addAll(outByteArray, buffer);
            }
 
            gZIPInputStream.close();
            return new String(outByteArray);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return "Error";
    }


}

