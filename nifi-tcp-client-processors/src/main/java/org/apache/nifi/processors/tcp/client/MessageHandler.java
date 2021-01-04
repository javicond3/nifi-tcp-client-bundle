package org.apache.nifi.processors.tcp.client;


import java.util.ArrayList;
import java.util.List;

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
	
	public List<Byte> popPacketFromBuffer(List<Byte> buffer){
        int bufferSize = buffer.size();

        // Return on small packet length
        if (bufferSize < 4) return null;

        if (buffer.get(bufferSize - 2) == dle && buffer.get(bufferSize - 1) == etx){

            // Check to see if we have an odd number of dle packets
            // An Even number will signify a dle stuffed mid packet!
            int dleCount = 0;
            for (int i = bufferSize - 2; i >= 0; i--){
                if (buffer.get(i) == dle){
                    dleCount += 1;
                }else{
                    break;
                }
            }

            if (dleCount % 2 == 0){
                // dleCount is an even number so this is not the end of a packet
                return null;
            }


            // Ensure packet begins with a valid startDelimiter
            if (buffer.get(0) != dle && buffer.get(1) != stx){
				System.out.println("Popped a packet without a valid start delimiter");
                return null;
            }

            return deStuffPacket(buffer);
        }

        return null;
    }
	
	

	public List<Byte> deStuffPacket(List<Byte> stuffedPacket){
        int lengthOfBuffer = stuffedPacket.size();

        List <Byte> buffer = new ArrayList<Byte>();
        for (int i = 0; i < lengthOfBuffer - 1; i++){
            if (stuffedPacket.get(i) == dle && stuffedPacket.get(i + 1) == dle){
                // Write a single dle byte!
                buffer.add((byte) dle);
                i++;
            } else if (stuffedPacket.get(i) == dle && stuffedPacket.get(i + 1) == stx){
                // Skip the header from the framing
                i++;
            } else if (stuffedPacket.get(i) == dle && stuffedPacket.get(i + 1) == etx){
                // Skip the footer from the framing
                i++;
            } else {
                // Write the byte as it's ok!
                buffer.add(stuffedPacket.get(i));
            }
        }

        return buffer;
    }

}

