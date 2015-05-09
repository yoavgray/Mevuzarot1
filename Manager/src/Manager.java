import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager {
	public static final String localAppQueue = "LocalAppQueue";
	public static final String workersQueue = "WorkersQueue";
	public static final String resultsQueue = "ResultsQueue";
	public static final String managerQueue = "ManagerQueue";
	public static final Integer maxNumberOfMessages = 1;
	public static boolean isTerminated, doneReadingMsgFromLocalApp;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
	public static int workerId;
	public static int numOfUrlsSent;
	public static ArrayList<String> WorkersIdList;
	public static HashMap<String, WorkerAssignment> workersAssignmentMap;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		isTerminated = false;
		doneReadingMsgFromLocalApp = false;
		// initialize all needed AWS services for the assignment
		initAmazonAwsServices();

		// start WorkersQueue and ResultsQueue
		startQueue(workersQueue);
		startQueue(resultsQueue);

		try {
			while (!isTerminated) {
				if (!doneReadingMsgFromLocalApp) {
					ProcessLocalAppMessage();
				} else {
					processWorkersMessages();
				}
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
			System.out.println("Stack Trace: " + ase.getStackTrace());
		} catch (AmazonClientException ace) {
			System.out.println("Error Message:" + ace.getMessage());
			System.out.println("Stack Trace: " + ace.getStackTrace());
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
							List<String> instanceIds = new ArrayList<String>();
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
		System.out
				.println("Now manager is supposed to look at the results queue and process messages");
		isTerminated = true;
	}

	private static void ProcessLocalAppMessage() throws AmazonServiceException,
			AmazonClientException, IOException {
		String[] body;
		numOfUrlsSent = 0;
		String fileName = null;
		String bucketName = null;
		int n = 0;

		//Parse message from Manager
		System.out.println("Receiving messages from LocalApp.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
				managerQueue);//.withMaxNumberOfMessages(maxNumberOfMessages);		//process one message each time
		List<Message> messages = sqsClient
				.receiveMessage(receiveMessageRequest).getMessages();
		String messageRecieptHandle = null;


		for (Message message : messages) {
			messageRecieptHandle = message.getReceiptHandle();
			String msg = message.getBody();
			body = msg.split("\t");
			if (body.length == 3) {
				bucketName = body[0];
				fileName = body[1];
				n = Integer.parseInt(body[2]);
			}
			else System.out.println("Manager - Error parsing message, missing bucketName or fileName or workersRatio");
		}

		// Delete the message from the queue
		System.out
				.println("Done reading message from LocalApp. Deleting the message.\n");
		sqsClient.deleteMessage(new DeleteMessageRequest(managerQueue,
				messageRecieptHandle));

		//Download the input file from S3 and parse it
		BufferedReader br = downloadInputFileFromS3(bucketName, fileName);

		String url;
		System.out.println("Starting to send work to workers...");
		while ((url = br.readLine()) != null) {
			sqsClient.sendMessage(new SendMessageRequest("WorkersQueue", url));
			numOfUrlsSent++;

			if (numOfUrlsSent % n == 0) {
				CreateNewWorkerInstance("t2.micro", "ami-1ecae776",
						"Ass1SecurityGroup");
			}

		}
		doneReadingMsgFromLocalApp = true;

	}

	private static void CreateNewWorkerInstance(String instanceType,
			String amiID, String securityGroup) {
		workerId++;
		RunInstancesRequest riReq = new RunInstancesRequest();
		riReq.setInstanceType(instanceType);
		riReq.setImageId(amiID);
		riReq.setKeyName("yoaveliran");
		riReq.setMinCount(Integer.valueOf(1));
		riReq.setMaxCount(Integer.valueOf(1));
		riReq.withSecurityGroupIds(securityGroup);
		riReq.setAdditionalInfo("Worker" + workerId);
		// riReq.setUserData(getUserDataScript());
		RunInstancesResult riRes = ec2Client.runInstances(riReq);
		List<String> instanceIdList = new ArrayList<String>();
		List<Tag> tagsList = new ArrayList<Tag>();
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
	private static BufferedReader downloadInputFileFromS3(String bucketName, String fileName) {
		System.out.println("Downloading InputFile from S3");
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
//		bucketName = credentials.getAWSAccessKeyId().toLowerCase();
		WorkersIdList = new ArrayList<String>();
	}

}
