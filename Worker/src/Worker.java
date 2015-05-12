import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.imageio.ImageIO;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

public class Worker {
	private static final int IMG_HEIGHT = 50;
	private static final int IMG_WIDTH = 50;
	private static final String WORKERS_QUEUE = "WorkersQueue";
	private static final String RESULTS_QUEUE = "ResultsQueue";
    private static final String MANAGER_QUEUE = "ManagerQueue";
    private static int numOfImageProcessed;

	public static boolean doneWork;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
	public static String id;
	public static ArrayList<String> failedUrls;

	public static void main(String[] args) throws IOException {
		doneWork = false;

		initAmazonAwsServices();

		while (!doneWork) {
			String[] body;
			String url;

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                    WORKERS_QUEUE);

			List<Message> messages = sqsClient.receiveMessage(
					receiveMessageRequest).getMessages();
			if (!messages.isEmpty()) {
				String messageReceiptHandle;
                String bucketName;
				for (Message message : messages) {
					messageReceiptHandle = message.getReceiptHandle();
					body = message.getBody().split("\t");
                    if (body.length == 2) {
                        bucketName = body[0];
                        url = body[1];
                        System.out.println(url);

                        new ChangeMessageVisibilityRequest(sqsClient.getQueueUrl(WORKERS_QUEUE).toString(), messageReceiptHandle, 15);
                        String fileToUpload = resizeImageFromUrl(new URL(url));
                        if (fileToUpload == null) {
                            System.out.println("Failed processing " + url + ". Continuing...");
                            sqsClient.deleteMessage(new DeleteMessageRequest(
                                    WORKERS_QUEUE, messageReceiptHandle));
                            // send failed message
                            sqsClient.sendMessage(new SendMessageRequest(RESULTS_QUEUE,
                                    bucketName + "\t" + "fail"));
                            sqsClient.deleteMessage(new DeleteMessageRequest(
                                    WORKERS_QUEUE, messageReceiptHandle));
                            continue;
                        }
                        File f = new File(fileToUpload);
                        // Upload the file
                        System.out.println("Bucket name is " + bucketName);
                        s3Client.putObject(new PutObjectRequest(bucketName, f.getName(), f));

                        //create a url to access the object
                        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, fileToUpload);
                        String newUrl = s3Client.generatePresignedUrl(generatePresignedUrlRequest).toString();

                        f.delete();
                        System.out.println("File uploaded and deleted.");

                        sqsClient.sendMessage(new SendMessageRequest(RESULTS_QUEUE,
                                bucketName + "\t" + url + "\t" + newUrl));
                    } else {
                        //got a termination message!
                        doneWork = true;
                    }
					sqsClient.deleteMessage(new DeleteMessageRequest(
                            WORKERS_QUEUE, messageReceiptHandle));
				}
			}
		}

        calculateStatistics();
        sqsClient.sendMessage(new SendMessageRequest(MANAGER_QUEUE, "terminate"));
        System.out.println("Worker " + id + " is supposed to terminate here!");
	}

    private static void calculateStatistics() {
        //TODO ELIRAN
        //this is where the worker "realizes" its terminating, so it should do all the statistics here
        // and then send the manager a terminating message
        System.out.println("Worker calculating statistics");
        //TODO ELIRAN upload to S3 a statistics file
    }

	private static void initAmazonAwsServices() throws IOException {
		// Set AWS credentials and create services
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		ec2Client = new AmazonEC2Client(credentials);
		s3Client = new AmazonS3Client(credentials);
		sqsClient = new AmazonSQSClient(credentials);
		failedUrls = new ArrayList<>();
		id = UUID.randomUUID().toString();

		System.out.println("Done initializing EC2, S3 & SQS");
	}

	public static String resizeImageFromUrl(URL imageUrl){
		BufferedImage originalImage;
        File file = null;
		try {
			originalImage = ImageIO.read(imageUrl);
			int type = originalImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB
					: originalImage.getType();
			BufferedImage result = getResizedImage(originalImage, type);

			String filePath = id + "_" + numOfImageProcessed
					+ ".jpg";
			file = new File(filePath);
			ImageIO.write(result, "jpg", file);
			numOfImageProcessed++;
			return filePath;
		} catch (IOException | NullPointerException e) {
			failedUrls.add(imageUrl.toString());
            if (file != null) {
                file.delete();
            }
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
