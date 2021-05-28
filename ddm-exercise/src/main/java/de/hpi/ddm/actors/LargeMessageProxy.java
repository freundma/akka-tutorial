package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletionStage;
import org.apache.commons.lang3.ArrayUtils;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.NotUsed;
import akka.stream.javadsl.*;
import akka.serialization.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.twitter.chill.KryoPool;
import de.hpi.ddm.singletons.KryoPoolSingleton;

import scala.concurrent.duration.Duration;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
    
	public static final String DEFAULT_NAME = "largeMessageProxy";
        
        //buffer for receiving large messages
        private byte[] buffer;
        
        //number of bytes to be sent as one chunk
        private final int chunkSize = 1000;
        
        enum Ack {
            INSTANCE;
        }
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}
        
        @Data @NoArgsConstructor @AllArgsConstructor
        public static class LargeMessageWrapper implements Serializable {
            private static final long serialVersionUID = -2254287805177870008L;
            private Object message;
            private String senderActorRef;
            private String receiverActorRef;
        }
        
        @Data @NoArgsConstructor @AllArgsConstructor
        public static class Chunk implements Serializable {
            private static final long serialVersionUID = -2631091331982674006L;
            private byte[] buffer;
        }
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
                                .match(StreamInitialized.class,
                                        init -> {
                                            this.log().info("Stream initialized");
                                            sender().tell(Ack.INSTANCE, self());
                                        })
                                .match(Chunk.class, this::handleChunk)
                                .match(StreamCompleted.class, this::handleStreamCompleted)
                                .match(StreamFailure.class, this::handleStreamFailure)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
                ActorRef receiverProxy = null;
		ActorSelection receiverProxySelection = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
                CompletionStage<ActorRef> futureActor = receiverProxySelection.resolveOneCS(Duration.create(2, "seconds"));
                try {
                    receiverProxy = futureActor.toCompletableFuture().get();
                } catch (Exception e) {
                    this.log().error("unable to get actorref from actorselection:" + e.getMessage());
                }
                
		
		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
                KryoPool kryoPool = KryoPoolSingleton.get();
                
                this.log().info("Starting serialization");
                
                //serialization of actorRefs, then serialization with kryo
                LargeMessageWrapper lMW = new LargeMessageWrapper(message, 
                        Serialization.serializedActorPath(sender),
                            Serialization.serializedActorPath(receiver));
                byte[] serializedObject = kryoPool.toBytesWithClass(lMW);
                
                this.log().info("Serialization done");
                
                List<Chunk> chunkBuffer = new ArrayList<>();
                int numChunks = serializedObject.length / chunkSize;
                int rest = serializedObject.length % chunkSize;
                
                // add the bytes chunkwise to the buffer
                for (int i = 0; i < numChunks; i++) {
                    chunkBuffer.add(new Chunk (Arrays.copyOfRange(serializedObject, 
                            i*chunkSize, (i+1)*chunkSize)));
                }
                
                //add the rest to the buffer
                if (rest != 0) {
                    chunkBuffer.add(new Chunk (Arrays.copyOfRange(serializedObject, 
                            numChunks*chunkSize, serializedObject.length)));
                }
                
                //initialize source with chunks
                Source<Chunk, NotUsed> messageSource = Source.from(chunkBuffer);
                
                Sink<Chunk , NotUsed> sink = Sink.<Chunk>actorRefWithBackpressure(
                        receiverProxy,
                        new StreamInitialized(),
                        Ack.INSTANCE,
                        new StreamCompleted(),
                        ex -> new StreamFailure(ex));
                
                // run stream
                messageSource.runWith(sink, this.context().system());     
	}
        
        private void handleChunk(Chunk c) {
            buffer = ArrayUtils.addAll(buffer, c.buffer);
            this.log().info("Received chunk of length {}", c.buffer.length);
            
            this.sender().tell(Ack.INSTANCE, self());
        }
        
        private void handleStreamCompleted(StreamCompleted s){
            this.log().info("Stream completed");
          
            //deserialize
            final KryoPool kryo = KryoPoolSingleton.get();
            LargeMessageWrapper lMW = (LargeMessageWrapper) kryo.fromBytes(buffer);
            
            //get message, receiver, sender
            Object message = lMW.getMessage();
            ActorRef sender = this.context().system().provider().resolveActorRef(lMW.getSenderActorRef());
            ActorRef receiver = this.context().system().provider().resolveActorRef(lMW.getReceiverActorRef());
            
            //send message to actual receiver
            receiver.tell(message, sender);
            
            //clear buffer for next message
            buffer = null;
            
        }
        
        private void handleStreamFailure(StreamFailure s) {
            this.log().error(s.getCause(), "Stream failed!");
            
            //clear buffer after failure
            buffer = null;
        }
        
        public static class StreamInitialized implements Serializable {
            private static final long serialVersionUID = 2940665245810221101L;
        }

        public static class StreamCompleted implements Serializable {
            private static final long serialVersionUID = 2940665245810221102L;
        }

        public static class StreamFailure implements Serializable {
            private static final long serialVersionUID = 2940665245810221103L;
            private final Throwable cause;

            public StreamFailure(Throwable cause) {
                this.cause = cause;
            }

            public Throwable getCause() {
                return cause;
            }
        }
}
