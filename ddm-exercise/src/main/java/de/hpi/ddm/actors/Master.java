package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
		this.hintTaskMessages = new LinkedList<>();
		this.workerHintMessageMap = new HashMap<>();
		this.passwordMessages = new ArrayList<>();
		this.idleWorkers = new LinkedList<ActorRef>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintResultMessage implements  Serializable {
		private static final long serialVersionUID = 4L;
		private String result;
		private int id;

	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordResultMessage implements  Serializable {
		private static final long serialVersionUID = 4L;
		private String result;
		private int id;

	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;
	private final Map<Integer , Worker> workerHintMessageMap;
	private final Queue<Worker.WelcomeMessage> hintTaskMessages; //TODO: create message for solving a hint
	private final Queue<ActorRef> idleWorkers;
	private final List<Worker.PasswordMessage> passwordMessages;
	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(PasswordResultMessage.class, this::handle)
				.match(HintResultMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(HintResultMessage hintResultMessage) {
		//Todo: send filter update to all workers that work on hints of id
		// and update Bloomfilter of hints in queue with id
	}

	private void handle(PasswordResultMessage passwordResultMessage) {
		String result = Integer.toString(passwordResultMessage.getId()).concat(";").concat(passwordResultMessage.result);
		this.collector.tell(new Collector.CollectMessage(result), this.self());
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {

		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		// Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		if (message.getLines().isEmpty()) {
			this.terminate();
			return;
		}
		
		for (String[] line : message.getLines()) {
			String charset = line[2];
			String passwordHash = line[3];
			int passwordLength = Integer.parseInt(line[4]);
			int id = Integer.parseInt(line[0]);

			//Todo: add Bloomfilter params
			Worker.PasswordMessage passwordMessage = new Worker.PasswordMessage( new BloomFilter(), charset, passwordHash, passwordLength, id);
			this.passwordMessages.add(passwordMessage);

			//place hint tasks in queue
			for(int i = 5; i < line.length; i++) {
				String hintHash = line[i];
				this.hintTaskMessages.add(new Worker.WelcomeMessage(new BloomFilter(), charset, hintHash, id));
			}
		}


		for( ActorRef worker : this.idleWorkers) {
			this.tellNextHintTask(worker);
		}
	}

	private void tellNextHintTask(ActorRef worker) {
		if(hintTaskMessages.isEmpty()){
			this.idleWorkers.add(worker);
			this.reader.tell(new Reader.ReadMessage(), this.self());
		} else {
			this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(this.hintTaskMessages.poll(), worker), this.self());
		}
	}

	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());

		//Assign some work to registering workers. Note that the processing of the global task might have already started.
		this.tellNextHintTask(this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
