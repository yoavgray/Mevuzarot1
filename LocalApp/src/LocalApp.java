import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class LocalApp {
    private static final int RUNNING = 16;
    private static final int PENDING = 0;
    public static final String MANAGER_QUEUE = "ManagerQueue";
    public static final String LOCAL_APP_QUEUE = "LocalAppQueue";
    public static final String INSTANCE_ID = "t2.micro";
    public static final String AMI_ID = "ami-1ecae776";
    public static final String SECURITY_GROUP = "Ass1SecurityGroup";
    public static final String KEY_PAIR_NAME = "yoaveliran";
    public static final String TERMINATE = "terminate";
    public static AmazonEC2 ec2Client;
    public static AmazonS3 s3Client;
    public static AmazonSQS sqsClient;
    public static String bucketName;
    public static int workersRatio;
    public static String htmlFile;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 3 || args.length > 4)
            throw new InvalidArgumentException("Invalid arguments");

        String inputFileName = args[0];
        String outputFileName = args[1];
        workersRatio = Integer.parseInt(args[2]);

        htmlFile = "<HTML>\n<HEAD><TITLE>Assignment 1</TITLE>\n</HEAD>\n<BODY>\n{dynamicPart}\n</BODY>\n</HTML>\n";

        // initialize all needed AWS services for the assignment
        initAmazonAwsServices();

        // upload input file to S3
        uploadInputFileToS3(inputFileName);

        //if not exists, creates a managerQueue & localAppQueue.
        startQueue(MANAGER_QUEUE);
        startQueue(LOCAL_APP_QUEUE);

        // send a "start" message to Manager through SQS
        String msg = bucketName + "\t" + inputFileName + "\t"  + outputFileName + "\t" + workersRatio;

        if (args.length == 4 && args[3].equals(TERMINATE)) {
            msg += "\t" + TERMINATE;
        }
        sendMessageToManager(msg);

        // initialize the manager node if it does not exist yet
        startManagerNode(INSTANCE_ID, AMI_ID, SECURITY_GROUP);

        // receive the "done" message from the Manager
        boolean isLocalAppDone = false;
        while (!isLocalAppDone) {
            isLocalAppDone = recieveMessageFromManager();
        }
        System.out.println("LocalApp " + bucketName + " is done! Finishing...");
    }

    private static boolean recieveMessageFromManager() throws IOException {
        // output file should like like this:
        // bucketName\n
        // workerId + '\t' + oldUrl1 + '\t' + newUrl1
        // workerId + '\t' + oldUrl2 + '\t' + newUrl2
        // ......
        // ......

        //Parse message from Manager
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                LOCAL_APP_QUEUE);
        List<Message> messages = sqsClient
                .receiveMessage(receiveMessageRequest).getMessages();
        String messageReceiptHandle;

        for (Message message : messages) {
            if (message != null) {
                messageReceiptHandle = message.getReceiptHandle();
                String msg = message.getBody();
                String[] info = msg.split("\t");

                // if its the same bucketName, then it was addressed to this LocalApp!
                if (info[0].equals(bucketName)) {
                    String fileName = info[1];
                    System.out.println("Downloading summary file from S3.\n");
                    StringBuilder result = new StringBuilder(10000);
                    BufferedReader br = downloadFileFromS3(bucketName, fileName);

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parsedLine = line.split("\t");
                        if (parsedLine.length == 2) {
                            result.append("<a href=\"").append(parsedLine[0]).append("\"><img src=\"").append(parsedLine[1]).append("\" width=\"50\" height=\"50\"></a>\n");
                        } else {
                            System.out.println("Problem with output file. Check structure!");
                        }
                    }

                    String finalHtmlFile = htmlFile.replace("{dynamicPart}", result.toString());

                    String htmlFileName = bucketName.substring(0,9) + "htmlOutputFile.html";

                    PrintWriter writer = new PrintWriter(htmlFileName, "UTF-8");
                    writer.println(finalHtmlFile);
                    writer.close();

                    System.out.println("HTML file ready for LocalApp " + bucketName);

                    // Delete the message from the queue
                    System.out
                            .println("Done reading outputFile from manager. Deleting the message.\n");
                    return true;
                }
            }
        }
        return false;
    }


	private static void sendMessageToManager(String message) {

		System.out.println("Sending manager the opening message.\n");

		String queueUrl = sqsClient.getQueueUrl(MANAGER_QUEUE).getQueueUrl();

		sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
	}

	private static void uploadInputFileToS3(String fileToUpload) {
		File f = new File(fileToUpload);
		PutObjectRequest por = new PutObjectRequest(bucketName, fileToUpload, f);
		// Upload the file
		s3Client.putObject(por);
		System.out.println("File uploaded.");
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

	private static void startManagerNode(String instanceType, String amiID,
			String securityGroup) {
		if (!isManagerNodeExists()) {
			System.out
					.println("Manager Node does not exist yet. Starting Manager node...");
			RunInstancesRequest riReq = new RunInstancesRequest();
			riReq.setInstanceType(instanceType);
			riReq.setImageId(amiID);
			riReq.setKeyName(KEY_PAIR_NAME);
			riReq.setMinCount(1);
			riReq.setMaxCount(1);
			riReq.withSecurityGroupIds(securityGroup);
			riReq.setUserData(getUserDataScript());
			RunInstancesResult riRes = ec2Client.runInstances(riReq);
			List<String> instanceIdList = new ArrayList<>();
			List<Tag> tagsList = new ArrayList<>();
			String insId = riRes.getReservation().getInstances().get(0)
					.getInstanceId();
			Tag newTag = new Tag(insId, "manager");
			tagsList.add(newTag);
			instanceIdList.add(insId);

			ec2Client.createTags(new CreateTagsRequest(instanceIdList, tagsList));
			System.out.println("Manager Node running...");
		}

	}

	private static String getUserDataScript() {
		ArrayList<String> lines = new ArrayList<>();
		lines.add("#! /bin/bash -ex");
        lines.add("wget https://s3.amazonaws.com/initialjarfilesforassignment1/Manager.zip");
        lines.add("unzip -P bubA2003 Manager.zip");
        lines.add("java -jar Manager.jar");

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
				if (instance.getState().getCode() == RUNNING// running
						|| instance.getState().getCode() == PENDING) // pending
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
		try {
            AWSCredentials credentials = new PropertiesCredentials(LocalApp.class.getResourceAsStream("AwsCredentials.properties"));

			ec2Client = new AmazonEC2Client(credentials);
			s3Client = new AmazonS3Client(credentials);
			sqsClient = new AmazonSQSClient(credentials);
			bucketName = UUID.randomUUID().toString();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/AwsCredentials.properties or ~/.aws/credentials), and is in valid format.",
					e);
		}
		if (!s3Client.doesBucketExist(bucketName)) {
			s3Client.createBucket(bucketName);
		}

		System.out.println("Initialized EC2, S3, SQS and created buckets "
				+ bucketName + " and results");
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

}
