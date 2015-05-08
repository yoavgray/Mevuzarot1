import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApp {
	public static final String managerQueue = "ManagerQueue";
	public static final String localAppQueue = "LocalAppQueue";

	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
	public static String bucketName, id;
	public static String instanceType;
	public static String amiID;
	public static String securityGroup;
	public static int numOfWorkers;

	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 3 || args.length > 4)
			throw new InvalidArgumentException("Invalid arguments");

		String inputFileName = args[0];
		String outputFileName = args[1];
		numOfWorkers = Integer.parseInt(args[2]);

		// initialize all needed AWS services for the assignment
		initAmazonAwsServices();

		// upload input file to S3
		uploadInputFileToS3(inputFileName);

		// send a "start" message to Manager through SQS
		sendMessageToManager(id + "\t" + numOfWorkers);

		// initialize the manager node if it does not exist yet
		startManagerNode("t2.micro", "ami-1ecae776", "Ass1SecurityGroup");

		// recieve the "done" message from the Manager
		// String messageFromManager = recieveMessageFromManager();

	}

	private static String recieveMessageFromManager() {
		// TODO Auto-generated method stub
		return null;
	}

	private static void sendMessageToManager(String message) {
		String queueUrl;

		System.out.println("Sending manager the opening message.\n");
		List<String> listQueuesUrls = sqsClient.listQueues(managerQueue)
				.getQueueUrls();
		if (listQueuesUrls.isEmpty()) {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(
					managerQueue);
			queueUrl = sqsClient.createQueue(createQueueRequest).getQueueUrl();
		} else {
			queueUrl = listQueuesUrls.get(0);
		}

		sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
	}

	private static void uploadInputFileToS3(String fileToUpload) {
		File f = new File(fileToUpload);
		PutObjectRequest por = new PutObjectRequest(bucketName, "InputFile", f);
		// Upload the file
		s3Client.putObject(por);
		System.out.println("File uploaded.");
	}

	private static void startManagerNode(String instanceType, String amiID,
			String securityGroup) {
		if (!isManagerNodeExists()) {
			System.out
					.println("Manager Node does not exist yet. Starting Manager node...");
			RunInstancesRequest riReq = new RunInstancesRequest();
			riReq.setInstanceType(instanceType);
			riReq.setImageId(amiID);
			riReq.setKeyName("yoaveliran");
			riReq.setMinCount(Integer.valueOf(1));
			riReq.setMaxCount(Integer.valueOf(1));
			riReq.withSecurityGroupIds(securityGroup);
			// riReq.setUserData(getUserDataScript());
			RunInstancesResult riRes = ec2Client.runInstances(riReq);
			List<String> instanceIdList = new ArrayList<String>();
			List<Tag> tagsList = new ArrayList<Tag>();
			String insId = riRes.getReservation().getInstances().get(0)
					.getInstanceId();
			Tag newTag = new Tag(insId, "manager");
			tagsList.add(newTag);
			instanceIdList.add(insId);

			ec2Client
					.createTags(new CreateTagsRequest(instanceIdList, tagsList));
			System.out.println("Manager Node running...");
		}

	}

	private static String getUserDataScript() {
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash -ex");
		// lines.add("wget https://s3.amazonaws.com/akiaj73yligsqbwt2fdq/manager.zip");
		// lines.add("unzip -P cHEdra3e manager.zip");
		lines.add("wget https://s3.amazonaws.com/akiaj4lyabwjftnf3jfa/manager.jar");
		lines.add("java -jar manager.jar");

		String str = new String(Base64.encodeBase64(join(lines, "\n")
				.getBytes()));
		return str;
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
	 * Checks if a node with the Tag "manager" is already active
	 * 
	 * @return true if the manager node is active. false otherwise.
	 */
	private static boolean isManagerNodeExists() {
		DescribeInstancesRequest dir = new DescribeInstancesRequest();
		List<Reservation> result = ec2Client.describeInstances(dir)
				.getReservations();

		for (Reservation reservation : result)
			for (Instance instance : reservation.getInstances())
				if (instance.getState().getCode() == 16 // running
						|| instance.getState().getCode() == 0) // pending
					for (Tag tag : instance.getTags())
						if (tag.getValue().equals("manager")) {
							System.out
									.println("Manager Node already exists. Continuing...");
							return true;
						}
		return false;
	}

	/**
	 * starts necessary AmazonAWS services for this assignment
	 */
	private static void initAmazonAwsServices() {
		AWSCredentials credentials;
		try {
			credentials = new PropertiesCredentials(
					LocalApp.class
							.getResourceAsStream("/AwsCredentials.properties"));
			ec2Client = new AmazonEC2Client(credentials);
			s3Client = new AmazonS3Client(credentials);
			sqsClient = new AmazonSQSClient(credentials);
			bucketName = credentials.getAWSAccessKeyId().toLowerCase();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		if (!s3Client.doesBucketExist(bucketName)) {
			s3Client.createBucket(bucketName);
		}

		id = UUID.randomUUID().toString();

		System.out.println("Initialized EC2, S3, SQS and created buckets "
				+ bucketName + " and results");
	}

}
