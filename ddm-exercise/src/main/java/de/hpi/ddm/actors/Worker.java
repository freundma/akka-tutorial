package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import java.util.ArrayList;
import java.util.Random;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;    // contains bitset with known information (which chars are already found)
                private String charset;             // the charset to work on
                private String hintHash;            // the hint hash to crack
                private int id;                     // id of the line
	}
        
        @Data @NoArgsConstructor @AllArgsConstructor
        public static class HintMessage implements Serializable {
            private static final long serialVersionUID = 1L; //TODO: Dafuer kann das commandline tool serialver benutzt werden
            private BloomFilter information;                 // information about the chars
            private int id;                                  // id of the line
        }
        
        public static class YieldMessage implements Serializable {
            private static final long serialVersionUID = 2L; //TODO
        }
        
        @Data @NoArgsConstructor @AllArgsConstructor
        public static class PasswordMessage implements Serializable {
            private static final long serialVersionUID = 3L; //TODO
            private BloomFilter allInformation;     // all information out of the hints
            private String charset;                 // charset to work in
            private String passwordHash;            // password hash to crack
            private int passwordLength;             // length of the password
            private int id;                         // ID of the line
        }
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
        private ActorRef master;
	private long registrationTime;
        private BloomFilter data;
        private String charset;
        private String hash;
        private String password;
        private int id;
        int index;
        
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
                                .match(YieldMessage.class, this::handle)
                                .match(HintMessage.class, this::handle)
                                .match(PasswordMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
                
                // get info out of message
                this.master = this.getSender();
                this.data = message.getWelcomeData();
                this.charset = message.getCharset();
                this.hash = message.getHintHash();
                this.id = message.getId();
                
                // start with random index
                Random r = new Random();
                this.index = r.nextInt(charset.length());
                calculate(this.index);
	}
        
        private void handle(YieldMessage message) {
            /* in this case we just progress with our hint cracking
               and simply take the next index of the charset */
            
	    // start from the next possible index	
            calculate(this.index+1);
        }
        
        private void handle(HintMessage message) {
            // we received a message with new information and add them to ours
            // Todo: check if id of hint belongs to current id ?! an old hint update could still be in inbox
            this.data.merge(message.getInformation());
            this.log().info("Received new Hint for id " + message.getId());
            
            // proceed with calculating, start from the next possible index
            calculate(this.index+1);
        }
        
        private void handle(PasswordMessage message) {
            
            this.charset = message.getCharset();
            this.hash = message.getPasswordHash();
            this.data = message.getAllInformation();
            this.id = message.getId();
            
            //generate final charset for password cracking
            String finalCharset = "";
            for (int i = 0; i < charset.length(); i++) {
                if (!this.data.getBits().get(i)) {
                    finalCharset = finalCharset + charset.charAt(i);
                }
            }
            
            /* TODO performance: Im Moment werden alle Kombinationen UND alle
               Hashes ausgerechnet, egal ob das Passwort schon recht früh gefunden wird.
               Besser: Alle Kombinationen ausrechnen (geht bei rekursiv auch nicht anders)
               und danach hashen bis Lösung geunden, so wie beim Hintknacken.
            */
            this.log().info("Starting to crack password: ID " + message.getId()
                            + " with charset " + finalCharset);
            crackPassword(finalCharset.toCharArray(), message.getPasswordLength());

            this.sender().tell(new Master.PasswordResultMessage(this.password, this.id), this.self());
        }
        
        private void calculate(int currentIndex) {
            // do one calculation round
            if (calculationRound(currentIndex)) {
                // we cracked the hash, we tell the master about our success, we are done
                this.master.tell(new HintMessage(this.data,this.id), this.self());
            } else {
                // we did not crack the hash, we yield to hear for eventually cracked hints
                this.self().tell(new YieldMessage(), this.getSelf());
            }
        }
        
        private boolean calculationRound(int currentIndex) {
            // find the next char to work with and create new charset without it
            this.index = findNext(currentIndex);
            char removal = this.charset.charAt(this.index);
            String modifiedCharset = this.charset.replace(String.valueOf(removal),"");
	
            // generate all permutations 
            this.log().info("Generating all permutations of charset with ID " + this.id + " without " + removal + " for hint " + this.hash);
            List<String> permutationList = new ArrayList<>();
            heapPermutation(modifiedCharset.toCharArray(), modifiedCharset.length(),
                               modifiedCharset.length(), permutationList);
            
            // hash all permutations and try to find a match
            for (String perm : permutationList){
                if (hash(perm).equals(this.hash)){
                    this.log().info("Solved a hint: " + removal + " does not occur in the password");
                    //set the index in our BloomFilter
                    this.data.getBits().set(this.index);
                    return true;
                }
            }
            return false;
        }
        
        private int findNext(int currentIndex) {
            int nextIndex = this.data.getBits().nextClearBit(currentIndex);
            if (nextIndex == -1 || nextIndex > this.charset.length() - 1) {
                nextIndex = this.data.getBits().nextClearBit(0);
            } 
            return nextIndex;
        }
        
        private void crackPassword(char[] set, int pwLength) {
            List<String> combinations = new ArrayList<>();
            
            // generate all combinations of with charset and given password length
            genAllCombinations(set, "", set.length, pwLength, combinations);
            
            // hash until we find the correct passwort
            for (String comb : combinations) {
                if (hash(comb).equals(this.hash)) {
                    this.password = comb;
                    return;
                }
            }
            
        }
	
        private void genAllCombinations(char[] set, String prefix, int n, int k, List <String> l) {
            // Base case: k is 0
            if (k == 0) {
                l.add(prefix);
            }
            // One by one add all characters
            // from set and recursively
            // call for k equals to k-1
            for (int i = 0; i < n; i++){
                // Next character of input added
                String newPrefix = prefix + set[i];
                // k is decreased, because
                // we have added a new character
                genAllCombinations(set, newPrefix, n, k-1,l);
            }
        }
        
	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}
