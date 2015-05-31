import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.*;

import javax.imageio.ImageIO;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
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
	private static final String STATS_BUCKET = "statisticsmrbrown";
    private static int numOfImageProcessed;

	public static boolean doneWork;
	public static boolean receivedTermination;
	public static AmazonEC2 ec2Client;
	public static AmazonS3 s3Client;
	public static AmazonSQS sqsClient;
	public static String id;
	public static ArrayList<String> failedUrls;
	public static ArrayList<String> successfulUrls;
	public static Date startTime;
	public static Date finishTime;
	public static long averageRunTimeOnSingleURL;
	public static int totalNumOfURLsHandled;
    private static HashMap jobsApplied;

    public static void main(String[] args) throws IOException {
		doneWork = false;
		receivedTermination = false;
		Calendar startCalendar = Calendar.getInstance();
		startTime = startCalendar.getTime();
        jobsApplied = new HashMap();

		initAmazonAwsServices();

		while (!doneWork) {
			String[] body;
			String url;

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
                    WORKERS_QUEUE);
            receiveMessageRequest.setMaxNumberOfMessages(1);

			List<Message> messages = sqsClient.receiveMessage(
					receiveMessageRequest).getMessages();
			if (!messages.isEmpty()) {
				String messageReceiptHandle;
				String bucketName;
				for (Message message : messages) {
					messageReceiptHandle = message.getReceiptHandle();
					body = message.getBody().split("\t");
					if (body.length == 2) {
                        //trying to avoid duplicate messages
                        if (jobsApplied.get(body) != null) {
                            sqsClient.deleteMessage(new DeleteMessageRequest(
                                    WORKERS_QUEUE, messageReceiptHandle));
                            continue;
                        }
						totalNumOfURLsHandled++;
						long msgStartTime = System.currentTimeMillis();

						bucketName = body[0];
						url = body[1];
						System.out.println(url);
                        jobsApplied.put(body,"applied");

						new ChangeMessageVisibilityRequest(sqsClient.getQueueUrl(WORKERS_QUEUE).toString(), messageReceiptHandle, 150);
						String fileToUpload = resizeImageFromUrl(new URL(url));
						if (fileToUpload == null) {
							System.out.println("Failed processing " + url + ". Continuing...");
							sqsClient.deleteMessage(new DeleteMessageRequest(
									WORKERS_QUEUE, messageReceiptHandle));
							// send failed message
							sqsClient.sendMessage(new SendMessageRequest(RESULTS_QUEUE,
									bucketName + "\t" + "fail"));
							continue;
						}
						File f = new File(fileToUpload);
						// Upload the file
						System.out.println("Bucket name is " + bucketName);
						s3Client.putObject(new PutObjectRequest(bucketName, f.getName(), f));

						//create a url to access the object
						GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, fileToUpload);
						String newUrl = s3Client.generatePresignedUrl(generatePresignedUrlRequest).toString();
						successfulUrls.add(url);
						averageRunTimeOnSingleURL += System.currentTimeMillis() - msgStartTime;

						f.delete();
						System.out.println("File uploaded and deleted.");

						sqsClient.sendMessage(new SendMessageRequest(RESULTS_QUEUE,
								bucketName + "\t" + url + "\t" + newUrl));
					} else {
						//got a termination message!
						receivedTermination = true;
                        continue;
					}
					sqsClient.deleteMessage(new DeleteMessageRequest(
							WORKERS_QUEUE, messageReceiptHandle));
				}
			} else {
				if (receivedTermination) {
					doneWork = true;
					System.out.println("Worker is Done working and there are no other messages for worker");
					Calendar finishCalendar = Calendar.getInstance();
					finishTime = finishCalendar.getTime();
				}

			}
		}

        calculateStatistics();
        sqsClient.sendMessage(new SendMessageRequest(MANAGER_QUEUE, "terminate"));
        System.out.println("Worker " + id + " is supposed to terminate here!");
	}

    private static void calculateStatistics() {
        //this is where the worker "realizes" its terminating, so it should do all the statistics here
        // and then send the manager a terminating message
        System.out.println("Worker calculating statistics");
		System.out.println("Creating Statistics File...");
		averageRunTimeOnSingleURL = averageRunTimeOnSingleURL / 1000;

		String statsFile = "WorkerID: "+ id + "\n Start Time: "+ startTime.toString() + "sec \n Average run-time on single URL: " + String.valueOf(averageRunTimeOnSingleURL) + "sec";
		statsFile += "\n Total Number of URLS handled: "+ String.valueOf(totalNumOfURLsHandled) + "\n Finish Time: " + finishTime.toString() + "sec";
		statsFile += "\n List Of Successful URL:";
		for (String str: successfulUrls) {
			statsFile += "\n" + str;
		}
		statsFile += "\n List Of Failed URL:";
		for (String str: failedUrls) {
			statsFile += "\n" + str;
		}

		PrintWriter writer;
		try {
			String fileName = "statistics_"+id+".txt";
			writer = new PrintWriter(fileName, "UTF-8");
			writer.println(statsFile);
			writer.close();
			uploadStatsFileToS3(fileName);

		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
	private static void uploadStatsFileToS3(String fileToUpload) {
		File f = new File(fileToUpload);
		PutObjectRequest por = new PutObjectRequest(STATS_BUCKET, fileToUpload, f);
		// Upload the file
		s3Client.putObject(por);
		System.out.println("File uploaded.");
        f.delete();
	}

	private static void initAmazonAwsServices() throws IOException {
		// Set AWS credentials and create services
		AWSCredentials credentials = new PropertiesCredentials(Worker.class.getResourceAsStream("AwsCredentials.properties"));
		ec2Client = new AmazonEC2Client(credentials);
		s3Client = new AmazonS3Client(credentials);
		sqsClient = new AmazonSQSClient(credentials);
		failedUrls = new ArrayList<>();
		successfulUrls = new ArrayList<>();
		totalNumOfURLsHandled = 0;
		averageRunTimeOnSingleURL = 0;
		id = UUID.randomUUID().toString();


		System.out.println("Done initializing EC2, S3 & SQS");
	}

	public static String resizeImageFromUrl(URL imageUrl){
		BufferedImage originalImage;
        File file = null;
		try {
			try {
				originalImage = ImageIO.read(imageUrl);
			} catch (ArrayIndexOutOfBoundsException e) {
				failedUrls.add(imageUrl.toString()+"\t Error message: "+e.getMessage());
				return null;
			}
			int type = originalImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB
					: originalImage.getType();
			BufferedImage result = getResizedImage(originalImage, type);

			String filePath = id + "_" + numOfImageProcessed
					+ ".jpg";
			file = new File(filePath);
			ImageIO.write(result, "jpg", file);
			numOfImageProcessed++;
			return filePath;
		} catch (IOException | RuntimeException e) {
			failedUrls.add(imageUrl.toString()+"\t Error message: "+e.getMessage());
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
