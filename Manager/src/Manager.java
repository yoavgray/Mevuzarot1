import java.io.*;
import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

public class Manager {
	public static final String LOCAL_APP_QUEUE = "LocalAppQueue";
	public static final String WORKERS_QUEUE = "WorkersQueue";
	public static final String RESULTS_QUEUE = "ResultsQueue";
	public static final String MANAGER_QUEUE = "ManagerQueue";
	public static final String TERMINATE = "terminate";
    public static final String INSTANCE_TYPE = "t2.micro";
    public static final String AMI_ID = "ami-1ecae776";
    public static final String SECURITY_GROUP = "Ass1SecurityGroup";
    public static final String KEY_PAIR_NAME = "assignment1";
    public static final String STATS_BUCKET = "statisticsmrbrown";
    public static HashMap<String,Integer> localAppJobsCounter;
    public static HashMap<String,StringBuilder> localAppSummaryFiles;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
    public static String outputFileName;
    public static boolean isTerminated;
    public static boolean isManagerDone;
    private static int numOfWorkersCreated;

	public static void main(String[] args) throws IOException {
		isTerminated = false;
        isManagerDone = false;
		numOfWorkersCreated = 0;
        localAppJobsCounter = new HashMap<>();
        localAppSummaryFiles = new HashMap<>();

        // initialize all needed AWS services for the assignment
        initAmazonAwsServices();

        // start WorkersQueue and ResultsQueue
		startQueue(WORKERS_QUEUE);
        startQueue(RESULTS_QUEUE);

        Thread t = new Thread(new RunnableWorkAssigner());
        t.start();

		try {
			while (!isManagerDone) {
                processWorkersMessages();
			}

            sendPoisonToWorkers();
            boolean areWorkersReadyToDie = false;
            while (!areWorkersReadyToDie) {
                areWorkersReadyToDie = waitForWorkersResponse();
            }

            TerminateAllWorkers();
            System.out.println("workers job is done. terminating manager instance");
            deleteAllSqsQueues();
            TerminateManager();

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Response Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
			System.out.println("Stack Trace: " + Arrays.toString(ase.getStackTrace()));
		} catch (AmazonClientException ace) {
			System.out.println("Error Message:" + ace.getMessage());
			System.out.println("Stack Trace: " + Arrays.toString(ace.getStackTrace()));
		}

    }

    private static boolean waitForWorkersResponse() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                MANAGER_QUEUE);
        List<Message> messages = sqsClient.receiveMessage(
                receiveMessageRequest).getMessages();

        String messageReceiptHandle;
        for (Message message : messages) {
            messageReceiptHandle = message.getReceiptHandle();
            new ChangeMessageVisibilityRequest(sqsClient.getQueueUrl(MANAGER_QUEUE).toString(), messageReceiptHandle, 5);
            System.out.println("WaitingForWorkersResponse: Got " + message.getBody());
            if (message.getBody().equals(TERMINATE)) {
                numOfWorkersCreated--;
                if (numOfWorkersCreated == 0) {
                    return true;
                }
            }

            sqsClient.deleteMessage(new DeleteMessageRequest(
                    MANAGER_QUEUE, messageReceiptHandle));
        }
        return false;
    }

    private static void sendPoisonToWorkers() {
        for (int i=0; i<numOfWorkersCreated; i++) {
            System.out.println("Sending poison to worker #" + i);
            sqsClient.sendMessage(new SendMessageRequest(
                    WORKERS_QUEUE, TERMINATE));
        }
    }

    // starts a new Queue if not exists already
	private static void startQueue(String queueName) {
		List<String> listQueuesUrls = sqsClient.listQueues(queueName)
				.getQueueUrls();
		if (listQueuesUrls.isEmpty()) {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(
					queueName);
			sqsClient.createQueue(createQueueRequest);
		}
	}

    private static void deleteAllSqsQueues() {
        sqsClient.deleteQueue(new DeleteQueueRequest(MANAGER_QUEUE));
        sqsClient.deleteQueue(new DeleteQueueRequest(WORKERS_QUEUE));
        sqsClient.deleteQueue(new DeleteQueueRequest(RESULTS_QUEUE));
    }

    private static void TerminateManager() {
		DescribeInstancesRequest req = new DescribeInstancesRequest();
		List<Reservation> result = ec2Client.describeInstances(req)
				.getReservations();
		for (Reservation reservation : result) {
			for (Instance instance : reservation.getInstances()) {
				if (instance.getState().getCode() == 16) { // running
					for (Tag tag : instance.getTags()) {
						// check if the instance has a manager tag
						if (tag.getValue().equals("manager")) {
							List<String> instanceIds = new ArrayList<>();
							instanceIds.add(instance.getInstanceId());
							ec2Client
									.terminateInstances(new TerminateInstancesRequest(
											instanceIds));
						}
					}
				}
			}
		}
	}

	private static void processWorkersMessages() {
        String[] body;

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                RESULTS_QUEUE);
        List<Message> messages = sqsClient.receiveMessage(
                receiveMessageRequest).getMessages();
        if (!messages.isEmpty()) {
            String messageReceiptHandle;
            for (Message message : messages) {
                messageReceiptHandle = message.getReceiptHandle();
                body = message.getBody().split("\t");
                String bucketName;
                int newVal = -1;
                if (body.length == 3) {
                    bucketName = body[0];
                    String oldUrl = body[1];
                    String newUrl = body[2];
                    String tmpString = oldUrl + "\t" + newUrl + "\n";

                    if (localAppSummaryFiles.get(bucketName) != null) {
                        localAppSummaryFiles.put(bucketName, localAppSummaryFiles.get(bucketName).append(tmpString));
                        newVal = localAppJobsCounter.get(bucketName) - 1;
                        System.out.println("1 job less for " + bucketName + ". still " + newVal + " to go!");
                        localAppJobsCounter.put(bucketName, newVal);
                    }
                } else {
                    bucketName = body[0];
                    if (localAppSummaryFiles.get(bucketName) != null) {
                        newVal = localAppJobsCounter.get(bucketName) - 1;
                        System.out.println("1 job less (failed) for " + bucketName + ". still " + newVal + " to go!");
                        localAppJobsCounter.put(bucketName, newVal);
                    }
                }

                sqsClient.deleteMessage(new DeleteMessageRequest(
                        RESULTS_QUEUE, messageReceiptHandle));

                // job is done?
                if (newVal == 0) {
                    try {
                        PrintWriter writer = new PrintWriter(outputFileName, "UTF-8");
                        String summaryFile = localAppSummaryFiles.get(bucketName).toString();
                        writer.println(summaryFile);
                        writer.close();
                        uploadFileToS3(bucketName, outputFileName);

                        String msgForLocalApp = bucketName + "\t" + outputFileName;
                        sqsClient.sendMessage(new SendMessageRequest(
                                LOCAL_APP_QUEUE, msgForLocalApp));
                        System.out.println("job for " + bucketName + " is over!");
                        localAppJobsCounter.remove(bucketName);
                        localAppSummaryFiles.remove(bucketName);
                        // means all workers are done
                        if (localAppSummaryFiles.size() == 0) {
                            //if a terminate msg has arrived, now is the time to go
                            isManagerDone = isTerminated;
                        }
                    } catch (FileNotFoundException | UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
	}

    private static void TerminateAllWorkers() {
        DescribeInstancesRequest req = new DescribeInstancesRequest();
        List<Reservation> result = ec2Client.describeInstances(req)
                .getReservations();
        for (Reservation reservation : result) {
            for (Instance instance : reservation.getInstances()) {
                if (instance.getState().getCode() == 16) { // running
                    for (Tag tag : instance.getTags()) {
                        // check if the instance has a manager tag
                        if (tag.getValue().equals("worker")) {
                            List<String> instanceIds = new ArrayList<>();
                            instanceIds.add(instance.getInstanceId());
                            ec2Client
                                    .terminateInstances(new TerminateInstancesRequest(
                                            instanceIds));
                        }
                    }
                }
            }
        }
    }

    private static void uploadFileToS3(String bucketName, String fileToUpload) {
        File f = new File(fileToUpload);
        PutObjectRequest por = new PutObjectRequest(bucketName, fileToUpload, f);
        // Upload the file
        s3Client.putObject(por);
        System.out.println("File uploaded.");
    }

    private static void processMessage(String thisBucketName, String fileName, int ratio) throws IOException {

        //Download the input file from S3 and parse it
        BufferedReader urlFile = downloadFileFromS3(thisBucketName, fileName);

        String line;
        List<String> lines = new ArrayList<>();
        while ((line = urlFile.readLine()) != null) {
            lines.add(line);
        }

        localAppJobsCounter.put(thisBucketName,lines.size());
        System.out.println("Job " + thisBucketName + " needs " + localAppJobsCounter.get(thisBucketName).toString() + " urls");
        int numOfWorkersNeededForJob = lines.size() / ratio + 1;
        // create the needed amount of workers
        createWorkersIfNeeded(numOfWorkersNeededForJob);

        System.out.println("Starting to send work to workers...");
        for (String url : lines) {
            sqsClient.sendMessage(new SendMessageRequest("WorkersQueue", thisBucketName + "\t" + url));
        }
    }

    private static synchronized void createWorkersIfNeeded(int numOfWorkersNeededForJob) {
        int k = numOfWorkersNeededForJob - getNumOfWorkersCreated();
        if (k > 0) {
            for (int i=0; i<k; i++) {
                CreateNewWorkerInstance(INSTANCE_TYPE, AMI_ID, SECURITY_GROUP);
            }
        }
    }

    private static void CreateNewWorkerInstance(String instanceType,
                                                String amiID, String securityGroup) {
        RunInstancesRequest riReq = new RunInstancesRequest();
        riReq.setInstanceType(instanceType);
        riReq.setImageId(amiID);
        riReq.setKeyName(KEY_PAIR_NAME);
        riReq.setMinCount(1);
        riReq.setMaxCount(1);
        riReq.withSecurityGroupIds(securityGroup);
        riReq.setUserData(getUserDataScript());
        RunInstancesResult riRes = ec2Client.runInstances(riReq);
        setNumOfWorkersCreated(getNumOfWorkersCreated() + 1);
        List<String> instanceIdList = new ArrayList<>();
        List<Tag> tagsList = new ArrayList<>();
        String insId = riRes.getReservation().getInstances().get(0)
                .getInstanceId();
        Tag newTag = new Tag(insId, "worker");
        tagsList.add(newTag);
        instanceIdList.add(insId);

        ec2Client.createTags(new CreateTagsRequest(instanceIdList, tagsList));
        System.out.println("Created another Worker Instance running...");
    }

    private static String getUserDataScript() {
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash -ex");
        lines.add("wget https://s3.amazonaws.com/initialjarfilesforassignment1/Worker.zip");
        lines.add("unzip -P bubA2003 Worker.zip");
        lines.add("java -jar Worker.jar");

        return new String(Base64.encodeBase64(join(lines, "\n")
                .getBytes()));
    }

    static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

	/**
	 * Download the input file from S3 after receiving the opening message from
	 * LocalApp
	 * 
	 * @param bucketName
	 *            the application bucket to download from
	 * @param fileName
	 * 			  filename to download from S3
	 * @return a BufferedReader instance with the input file's data
	 */
	private static BufferedReader downloadFileFromS3(String bucketName, String fileName) {
		System.out.println("Downloading " + fileName + " from S3");
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName,
				fileName));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				s3Object.getObjectContent()));
		s3Client.deleteObject(bucketName, fileName);
		return reader;
	}

	private static void initAmazonAwsServices() throws IOException {
		// Set AWS credentials and create services

        AWSCredentials credentials = new PropertiesCredentials(Manager.class.getResourceAsStream("AwsCredentials.properties"));
		ec2Client = new AmazonEC2Client(credentials);
		s3Client = new AmazonS3Client(credentials);
		sqsClient = new AmazonSQSClient(credentials);

        if (!s3Client.doesBucketExist(STATS_BUCKET)) {
            s3Client.createBucket(STATS_BUCKET);
        }
	}

    private static void listenToManagerQueueAndAssignWork() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                MANAGER_QUEUE);
        List<Message> messages = sqsClient
                .receiveMessage(receiveMessageRequest).getMessages();
        String messageRecieptHandle;

        for (Message message : messages) {
            if (message != null ) {
                String thisBucketName;
                messageRecieptHandle = message.getReceiptHandle();
                String msg = message.getBody();
                String[] body = msg.split("\t");
                if (body.length >= 4) {
                    thisBucketName = body[0];
                    String fileName = body[1];
                    outputFileName = body[2];
                    int ratio = Integer.parseInt(body[3]);

                    if (body.length == 5 && body[4].equals(TERMINATE)) {
                        isTerminated = true;
                    }

                    localAppSummaryFiles.put(thisBucketName,new StringBuilder(5000));

                    //create a new thread to download input file from S3 and assign work to workers
                    Thread t = new Thread(new LocalAppJobHandler(thisBucketName, fileName, ratio));
                    t.start();

                    // Delete the message from the queue
                    System.out
                            .println("Done receiving a task from LocalApp:" + thisBucketName + ". Deleting the message and waiting for the next one.\n");
                    sqsClient.deleteMessage(new DeleteMessageRequest(MANAGER_QUEUE,
                            messageRecieptHandle));
                }
            }
        }

    }

    public static int getNumOfWorkersCreated() {
        return numOfWorkersCreated;
    }

    public static void setNumOfWorkersCreated(int numOfWorkersCreated) {
        Manager.numOfWorkersCreated = numOfWorkersCreated;
    }

    public static class RunnableWorkAssigner implements Runnable {

        @Override
        public void run() {
            System.out.println("RunnableWorkAssigner started listening to Manager queue!");
            while (!isTerminated) {
                listenToManagerQueueAndAssignWork();
            }
            System.out.println("RunnableWorkAssigner is terminated! not listening to manager queue anymore!");
        }
    }

    public static class LocalAppJobHandler implements Runnable {
        private String bucketName;
        private String fileName;
        private int ratio;

        public LocalAppJobHandler(String bucketName, String fileName, int ratio) {
            this.bucketName = bucketName;
            this.fileName = fileName;
            this.ratio = ratio;
        }

        @Override
        public void run() {
            try {
                processMessage(bucketName, fileName, ratio);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
