package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.structures.AllPermutation;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

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
        this.idle = true;
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintTaskMessage implements Serializable {
        private static final long serialVersionUID = 5725612770463675298L;
        private BloomFilter hintData;    // contains bitset with known information (which chars are already found)
        private String charset;             // the charset to work on
        private String hintHash;            // the hint hash to crack
        private int id;                     // id of the line
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WelcomeMessage implements Serializable {
        private static final long serialVersionUID = 3725473466837766112L;
        private BloomFilter welcomeData;    // contains bitset with known information (which chars are already found)
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintResultMessage implements Serializable {
        private static final long serialVersionUID = 718838000998610000L;
        private BloomFilter information;                 // information about the chars
        private int id;                                  // id of the line
    }

    public static class YieldMessage implements Serializable {
        private static final long serialVersionUID = -3163881007848631811L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordMessage implements Serializable {
        private static final long serialVersionUID = 3324759103663001798L;
        private BloomFilter allInformation;     // all information out of the hints
        private String charset;                 // charset to work with
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
    private int index;
    private boolean idle;

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
                .match(HintTaskMessage.class, this::handle)
                .match(YieldMessage.class, this::handle)
                .match(HintResultMessage.class, this::handle)
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

    }

    private void handle(HintTaskMessage message) {
        this.log().info("HintTaskMessage received");
        this.idle = false;

        // get info out of message
        this.master = this.getSender();
        this.data = message.getHintData();
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
        if (!this.idle) calculate(this.index + 1);
    }

    private void handle(HintResultMessage message) {
        // we received a message with new information and add them to ours
        // Todo: check if id of hint belongs to current id ?! an old hint update could still be in inbox
        this.data.merge(message.getInformation());
        this.log().info("Received new Hint for id " + message.getId());

        // proceed with calculating, start from the next possible index
        calculate(this.index + 1);
    }

    private void handle(PasswordMessage message) {

        this.charset = message.getCharset();
        this.hash = message.getPasswordHash();
        this.data = message.getAllInformation();
        this.id = message.getId();

        //generate final charset for password cracking
        StringBuilder finalCharset = new StringBuilder();
        for (int i = 0; i < charset.length(); i++) {
            if (!this.data.getBits().get(i)) {
                finalCharset.append(charset.charAt(i));
            }
        }
  
        this.log().info("Starting to crack password: ID " + message.getId()
                + " with charset " + finalCharset);
        crackPassword(finalCharset.toString().toCharArray(), message.getPasswordLength());

        //this.sender().tell(new Master.PasswordResultMessage(this.password, this.id), this.self());
        this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Master.PasswordResultMessage(this.password,
                this.id), this.sender()), this.self());
    }

    private void calculate(int currentIndex) {
        // do one calculation round
        if (calculationRound(currentIndex)) {
            // we cracked the hash, we tell the master about our success, we are done
            // this.master.tell(new HintMessage(this.data, this.id), this.self());
            this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new HintResultMessage(this.data,
                this.id), this.master), this.self());
            this.idle = true;
        } else {
            // we did not crack the hash, we yield to hear for eventually cracked hints
            this.self().tell(new YieldMessage(), this.getSelf());
        }
    }

    private boolean calculationRound(int currentIndex) {
        // find the next char to work with and create new charset without it
        this.index = findNext(currentIndex);
        char removal = this.charset.charAt(this.index);
        String modifiedCharset = this.charset.replace(String.valueOf(removal), "");

        this.log().info("Trying to crack hint with ID " + this.id + " without " + removal + " for hint " + this.hash);

        // hash all permutations and try to find a match
        if (crackHint(modifiedCharset.toCharArray())) {
            this.log().info("Solved a hint: " + removal + " does not occur in the password");
            return true;
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
        int[] pos = new int[pwLength];
        // allocate a char array to hold the current combination:
        char[] combo = new char[pwLength];
        // initialize to the first value:
        for (int i = 0; i < pwLength; i++)
            combo[i] = set[0];

        while (true) {
            String password = new String(combo);
            if (hash(password).equals(this.hash)) {
                this.log().info("found password: " + password);
                this.password = password;
                return;
            }

            // move on to the next combination:
            int place = pwLength - 1;
            while (place >= 0) {
                if (++pos[place] == set.length) {
                    // overflow, reset to zero
                    pos[place] = 0;
                    combo[place] = set[0];
                    place--; // and carry across to the next value
                } else {
                    // no overflow, just set the char value and we're done
                    combo[place] = set[pos[place]];
                    break;
                }
            }
            if (place < 0)
                break;  // overflowed the last position, no more combinations
        }

    }

    private String hash(String characters) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));

            StringBuilder stringBuffer = new StringBuilder();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
    private boolean crackHint(char[] charset) {
        AllPermutation perm = new AllPermutation(charset);
        String permOutput = perm.GetFirst();
        if (hash(permOutput).equals(this.hash)) {
            this.data.getBits().set(this.index);
            return true;
        }
        while (perm.HasNext()) {
            permOutput = perm.GetNext();
            if (hash(permOutput).equals(this.hash)) {
                this.data.getBits().set(this.index);
                return true;
            }
        }
        return false;
    }
        
}
