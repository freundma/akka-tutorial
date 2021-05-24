package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import akka.actor.*;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";
	private boolean noMoreBatches;

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
		this.hintTasksQueue = new LinkedList<>();
		this.passwordTasksQueue = new LinkedList<>();
		this.workerHintMessageMap = new HashMap<>();
		this.passwordMessages = new ArrayList<>();
		this.idleWorkers = new LinkedList<>();
		this.receivedFirstBatch = false;
		this.noMoreBatches = false;
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
	private final Map<Integer , List<ActorRef>> workerHintMessageMap;
	private final Queue<Worker.WelcomeMessage> hintTasksQueue;
	private final Queue<ActorRef> idleWorkers;
	private final List<Worker.PasswordMessage> passwordMessages;
	private final Queue<Worker.PasswordMessage> passwordTasksQueue;
	private long startTime;
	private boolean receivedFirstBatch;
	
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
				.match(Worker.HintMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(Worker.HintMessage hintMessage) {
		this.log().info("Received Hint from " + this.sender());

		int id = hintMessage.getId();
		Optional<Worker.PasswordMessage> passwordMessage = this.passwordMessages.stream().filter(m -> m.getId() == id).findAny();
		passwordMessage.ifPresent(message -> {

			// update password bloom filter
			message.getAllInformation().merge(hintMessage.getInformation());

			// send filter update to all workers that work on hints of id
			List<ActorRef> workersToBeUpdated = this.workerHintMessageMap.get(id).stream().filter(actorRef -> !actorRef.equals(this.sender())).collect(Collectors.toList());
			// if all hints are present  to given information and put into queue
			if (workersToBeUpdated.isEmpty() && this.hintTasksQueue.stream().noneMatch(task -> task.getId() == id)) {
				this.passwordTasksQueue.add(message);
			} else {
				for (ActorRef worker : workersToBeUpdated) {
					worker.tell(new Worker.HintMessage(message.getAllInformation(), id), this.self());
				}

				for (Worker.WelcomeMessage hintTaskMessage : this.hintTasksQueue) {
					if (hintTaskMessage.getId() == id) {
						hintTaskMessage.setWelcomeData(message.getAllInformation());
					}
				}
				// remove worker from workerHintMap
			}
			this.workerHintMessageMap.replace(id, workersToBeUpdated);
			if(workersToBeUpdated.isEmpty()) this.workerHintMessageMap.remove(id);
		});

		// tell next Task to Worker
		this.tellNextTask(this.sender());
	}

	private void handle(PasswordResultMessage passwordResultMessage) {
		String result = Integer.toString(passwordResultMessage.getId()).concat(";").concat(passwordResultMessage.result);
		this.collector.tell(new Collector.CollectMessage(result), this.self());
		Optional<Worker.PasswordMessage> messageToRemove = this.passwordMessages.stream().filter(m -> m.getId() == passwordResultMessage.getId()).findAny();
		messageToRemove.ifPresent(this.passwordMessages::remove);

		this.tellNextTask(this.sender());
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
		this.log().info("Received new BatchMessage");
		if (message.getLines().isEmpty()) {
			this.log().info("No more batches to process, waiting for last tasks to finish");
			noMoreBatches = true;
		} else {

			for (String[] line : message.getLines()) {
				int id = Integer.parseInt(line[0]);
				String charset = line[2];
				int passwordLength = Integer.parseInt(line[3]);
				String passwordHash = line[4];

				Worker.PasswordMessage passwordMessage = new Worker.PasswordMessage(new BloomFilter(charset.length(), false), charset, passwordHash, passwordLength, id);
				this.passwordMessages.add(passwordMessage);

				int firstHintIndex = 5;
				//place hint tasks in queue
				for (int i = firstHintIndex; i < line.length; i++) {
					String hintHash = line[i];
					this.hintTasksQueue.add(new Worker.WelcomeMessage(new BloomFilter(charset.length(), false), charset, hintHash, id));
				}
			}
		}

		while ((!this.hintTasksQueue.isEmpty() || !this.passwordTasksQueue.isEmpty()) && !this.idleWorkers.isEmpty()){
			this.tellNextTask(this.idleWorkers.remove());
		}

	}

	private void tellNextTask(ActorRef receiver) {
		if (!this.passwordTasksQueue.isEmpty()) {
			this.tellNextPasswordTask(receiver);
		} else if (!this.hintTasksQueue.isEmpty()) {
			this.tellNextHintTask(receiver);
		}else {
			if (this.noMoreBatches && passwordMessages.isEmpty()) {
				terminate();
				return;
			}

			this.log().info("Set " + receiver + " to idle mode");
			this.idleWorkers.add(receiver);

			if (!noMoreBatches && this.receivedFirstBatch) {
				this.log().info("Requesting new batch from reader");
				this.reader.tell(new Reader.ReadMessage(), this.self());
			}
		}

	}

	private void tellNextHintTask(ActorRef receiver) {
		Worker.WelcomeMessage task = this.hintTasksQueue.remove();
		receiver.tell(task, this.self());
		//this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(task, receiver), this.self());

		// put worker into id pool
		this.addWorkerToHintPool(task.getId(), receiver);
	}

	private void addWorkerToHintPool(int id, ActorRef worker) {
		if (this.workerHintMessageMap.containsKey(id)) {
			this.workerHintMessageMap.get(id).add(worker);
		} else {
			List<ActorRef> workers = new ArrayList<>();
			workers.add(worker);
			this.workerHintMessageMap.put(id, workers);
		}
	}

	private void tellNextPasswordTask(ActorRef receiver) {
		// poll next password message that has all information
		receiver.tell(this.passwordTasksQueue.remove(), this.self());
		//this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(this.passwordTasksQueue.poll(), receiver), this.self());
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
		this.tellNextTask(this.sender());
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
