import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.imageio.ImageIO;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker {
	private static final int IMG_HEIGHT = 50;
	private static final int IMG_WIDTH = 50;
	private static final String workerQueue = "WorkersQueue";
	private static final String resultsQueue = "ResultsQueue";
	private static int numOfImageProcessed;

	public static boolean doneWork;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
	public static String id;
	public static String bucketName;
	public static String workerId;
	public static ArrayList<String> failedUrls;

	public static void main(String[] args) throws IOException {
		doneWork = false;

		initAmazonAwsServices();

		while (!doneWork) {
			String[] body;
			String url;

			System.out.println("Receiving messages from Manager.\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					"WorkersQueue");
			List<Message> messages = sqsClient.receiveMessage(
					receiveMessageRequest).getMessages();
			if (!messages.isEmpty()) {
				String messageRecieptHandle = null;
				for (Message message : messages) {
					messageRecieptHandle = message.getReceiptHandle();
					url = message.getBody();
					System.out.println(url);

					String fileToUpload = resizeImageFromUrl(new URL(url));
					if (fileToUpload == null) {
						System.out.println("Failed processing " + url + ". Continuing...");
						sqsClient.deleteMessage(new DeleteMessageRequest(
								workerQueue, messageRecieptHandle));
						continue;
					}
					File f = new File(fileToUpload);
					// Upload the file
					s3Client.putObject(new PutObjectRequest(bucketName, f.getName(), f));
					String newUrl = "https://s3.amazonaws.com/" + bucketName
							+ "/" + fileToUpload;
					f.delete();
					System.out.println("File uploaded and deleted.");

					sqsClient.sendMessage(new SendMessageRequest(resultsQueue,
							url + "\t" + newUrl));

					sqsClient.deleteMessage(new DeleteMessageRequest(
							workerQueue, messageRecieptHandle));
				}
			} else {
				System.out
						.println("No more messages for worker! Terminating :)");
				//TODO return this: TerminateWorker();
				
				doneWork = true;
			}
		}

	}

	private static void TerminateWorker() {
		DescribeInstancesRequest req = new DescribeInstancesRequest();
		List<Reservation> result = ec2Client.describeInstances(req)
				.getReservations();
		for (Reservation reservation : result) {
			for (Instance instance : reservation.getInstances()) {
				if (instance.getState().getCode() == 16) { // running
					for (Tag tag : instance.getTags()) {
						// check if the instance has a manager tag
						if (tag.getValue().equals("worker")) {
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

	private static void initAmazonAwsServices() throws IOException {
		// Set AWS credentials and create services
		AWSCredentials credentials = new PropertiesCredentials(
				Worker.class.getResourceAsStream("AwsCredentials.properties"));
		ec2Client = new AmazonEC2Client(credentials);
		s3Client = new AmazonS3Client(credentials);
		sqsClient = new AmazonSQSClient(credentials);
		bucketName = credentials.getAWSAccessKeyId().toLowerCase();
		failedUrls = new ArrayList<String>();
		id = new Instance().getInstanceId() == null ? "kaki" : new Instance().getInstanceId();

		System.out.println("Done initializing EC2, S3 & SQS");
	}

	public static String resizeImageFromUrl(URL imageUrl){
		BufferedImage originalImage;
		try {
			originalImage = ImageIO.read(imageUrl);
			int type = originalImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB
					: originalImage.getType();
			BufferedImage result = getResizedImage(originalImage, type);

			String filePath = "resizedImage_" + id + "_" + numOfImageProcessed
					+ ".jpg";
			File file = new File(filePath);
			ImageIO.write((RenderedImage) result, "jpg", file);
			numOfImageProcessed++;
			return filePath;
		} catch (IOException e) {
			failedUrls.add(imageUrl.toString());
		} catch (NullPointerException e) {
			failedUrls.add(imageUrl.toString());
		}
		return null;
	}

	public static BufferedImage getResizedImage(BufferedImage originalImage,
			int type) {

		BufferedImage resizedImage = new BufferedImage(IMG_WIDTH, IMG_HEIGHT,
				type);
		Graphics2D g = resizedImage.createGraphics();
		g.drawImage(originalImage, 0, 0, IMG_WIDTH, IMG_HEIGHT, null);
		g.dispose();

		return resizedImage;
	}
}