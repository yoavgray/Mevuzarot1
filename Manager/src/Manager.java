import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager {
	public static final String localAppQueue = "LocalAppQueue";
	public static final String workersQueue = "WorkersQueue";
	public static final String resultsQueue = "ResultsQueue";
	public static final String managerQueue = "ManagerQueue";
	public static final String TERMINATE = "terminate";
    public static final String INSTANCE_TYPE = "t2.micro";
    public static final String AMI_ID = "ami-1ecae776";
    public static final String SECURITY_GROUP = "Ass1SecurityGroup";
    public static final String KEY_PAIR_NAME = "yoaveliran";
    public static ConcurrentHashMap<String,Integer> localAppJobsCounter;
    public static ConcurrentHashMap<String,StringBuilder> localAppSummaryFiles;
    public static String bucketName;
    public static boolean isTerminated;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
    private static int numOfWorkersCreated;

	public static void main(String[] args) throws IOException {
		isTerminated = false;
		// initialize all needed AWS services for the assignment
		initAmazonAwsServices();
        numOfWorkersCreated = 0;
        localAppJobsCounter = new ConcurrentHashMap<>();
        localAppSummaryFiles = new ConcurrentHashMap<>();

		// start WorkersQueue and ResultsQueue
		startQueue(workersQueue);
		startQueue(resultsQueue);

        Thread t = new Thread(new RunnableWorkAssigner());
        t.start();

        System.out.println("1");
		try {
			while (!isTerminated) {
                processWorkersMessages();
			}

        System.out.println("workers job is done. terminating manager instance");
        //check there is not duplications in results file
        //termination upon argument
        cleanUpAndSendResponse();

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

	private static void cleanUpAndSendResponse() {
		TerminateManager();
		
		//sqsClient.deleteQueue(new DeleteQueueRequest(managerQueue));
		//sqsClient.deleteQueue(new DeleteQueueRequest(workersQueue));
		//sqsClient.deleteQueue(new DeleteQueueRequest(resultsQueue));
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
                resultsQueue);
        List<Message> messages = sqsClient.receiveMessage(
                receiveMessageRequest).getMessages();
        if (!messages.isEmpty()) {
            String messageReceiptHandle;
            for (Message message : messages) {
                messageReceiptHandle = message.getReceiptHandle();
                body = message.getBody().split("\t");
                bucketName = body[0];
                String oldUrl = body[1];
                String newUrl = body[2];
                String tmpString = oldUrl + "\t" + newUrl + "\n";
                localAppSummaryFiles.put(bucketName,localAppSummaryFiles.get(bucketName).append(tmpString));
                int newVal = localAppJobsCounter.get(bucketName) - 1;
                localAppJobsCounter.put(bucketName, newVal);

                sqsClient.deleteMessage(new DeleteMessageRequest(
                        resultsQueue, messageReceiptHandle));

                // job is done?
                if (newVal == 0) {
                    String fileName = "outputFile.txt";
                    try {
                        PrintWriter writer = new PrintWriter(fileName, "UTF-8");
                        String summaryFile = localAppSummaryFiles.get(bucketName).toString();
                        writer.println(summaryFile);
                        writer.close();
                        uploadFileToS3(fileName);

                        String msgForLocalApp = bucketName + "\t" + fileName;
                        sqsClient.sendMessage(new SendMessageRequest(
                                localAppQueue, msgForLocalApp));

                        localAppJobsCounter.remove(bucketName);
                        localAppSummaryFiles.remove(bucketName);
                        // means all workers are done
                        if (localAppSummaryFiles.size() == 0) {
                            TerminateAllWorkers();
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

    private static void uploadFileToS3(String fileToUpload) {
        File f = new File(fileToUpload);
        PutObjectRequest por = new PutObjectRequest(bucketName, fileToUpload, f);
        // Upload the file
        s3Client.putObject(por);
        System.out.println("File uploaded.");
    }

    private static void processMessage(String bucketName, String fileName, int ratio) throws IOException {

        //Download the input file from S3 and parse it
        BufferedReader urlFile = downloadFileFromS3(bucketName, fileName);

        String line;
        List<String> lines = new ArrayList<>();
        while ((line = urlFile.readLine()) != null) {
            lines.add(line);
        }

        localAppJobsCounter.put(bucketName,lines.size());
        int numOfWorkersNeededForJob = lines.size() / ratio + 1;
        // create the needed amount of workers
        createWorkersIfNeeded(numOfWorkersNeededForJob);

        System.out.println("Starting to send work to workers...");
        for (String url : lines) {
            sqsClient.sendMessage(new SendMessageRequest("WorkersQueue", bucketName + "\t" + url));
        }
    }

    private static synchronized void createWorkersIfNeeded(int numOfWorkersNeededForJob) {
        int k = numOfWorkersNeededForJob - getNumOfWorkersCreated();
        if (k > 0) {
            for (int i=0; i<k; i++) {
                CreateNewWorkerInstance(INSTANCE_TYPE, AMI_ID, SECURITY_GROUP);
            }
            System.out.println("Created " + k + " workers to handle " + bucketName + " job." );
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
        // riReq.setUserData(getUserDataScript());
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

		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		ec2Client = new AmazonEC2Client(credentials);
		s3Client = new AmazonS3Client(credentials);
		sqsClient = new AmazonSQSClient(credentials);
	}

    private static void listenToManagerQueueAndAssignWork() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                managerQueue);
        List<Message> messages = sqsClient
                .receiveMessage(receiveMessageRequest).getMessages();
        String messageRecieptHandle;

        for (Message message : messages) {
            if (message != null ) {
                messageRecieptHandle = message.getReceiptHandle();
                String msg = message.getBody();
                String[] body = msg.split("\t");
                if (body.length >= 3) {
                    bucketName = body[0];
                    String fileName = body[1];
                    int ratio = Integer.parseInt(body[2]);
                    localAppSummaryFiles.put(bucketName,new StringBuilder(5000));
                    //create a new thread to download input file from S3 and assign work to workers
                    Thread t = new Thread(new LocalAppJobHandler(bucketName, fileName, ratio));
                    t.start();

                    // Delete the message from the queue
                    System.out
                            .println("Done receiving a task from LocalApp:" + bucketName + ". Deleting the message and waiting for the next one.\n");
                    sqsClient.deleteMessage(new DeleteMessageRequest(managerQueue,
                            messageRecieptHandle));
                }
                if (body.length > 3 && body[3] != null) {
                    isTerminated = true;
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
