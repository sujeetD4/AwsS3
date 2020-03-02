package com.aws.quickstart;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;

public class CreateBucket {
	static AmazonS3 s3 = null;
	static {
		// final SecretKey secretKey = KeyGenerator.getInstance("AES").generateKey();
		s3 = AmazonS3ClientBuilder.standard().withRegion("") //Region goes here
				// .withCryptoConfiguration(new
				// CryptoConfiguration(CryptoMode.AuthenticatedEncryption))
				// .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(new
				// EncryptionMaterials(secretKey)))
				.build();

	}

	public static Bucket getBucket(String bucket_name) {

		Bucket named_bucket = null;
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			if (b.getName().equals(bucket_name)) {
				named_bucket = b;
			}
		}
		return named_bucket;
	}

	public static Bucket createBucket(String bucket_name) {
		System.out.format("\nCreating S3 bucket: %s\n", bucket_name);
		Bucket b = null;
		if (s3.doesBucketExistV2(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			b = getBucket(bucket_name);
		} else {
			try {
				b = s3.createBucket(bucket_name);
			} catch (AmazonS3Exception e) {
				System.err.println(e.getErrorMessage());
			}
		}

		if (b == null) {
			System.out.println("Error creating bucket!\n");
		} else {
			System.out.println("Done!\n");
		}

		return b;
	}

	public static void populate(String bucket, String today, List<String> files) {
		Bucket b = null;

		try {
			b = getBucket(bucket);
		} catch (AmazonS3Exception e) {
			System.err.println(e.getErrorMessage());
		}

		if (null != b) {
			System.out.println("putting objects");

			files.stream()
					.forEach(file -> s3.putObject(bucket,
							today + "/" + file.split("--->")[1].split(".csv")[0] + "/" + file.split("--->")[1],
							new File(file.split("--->")[0])));

		} else {
			System.out.println("No bucket found -" + bucket);
		}
	}

	public static void main(String[] args) {

		String today = new SimpleDateFormat("dd-MM-yyyy").format(new Date());

		String bucket_name = "";//bucket_name

		try {
			createBucket(bucket_name);
			populate(bucket_name, today, getFiles());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<String> getFiles() throws IOException {
		List<String> files = new ArrayList<>();
		//Directory for the csv files goes in Paths.get("")
		Files.walk(Paths.get("")).filter(Files::isRegularFile)
				.forEach(file -> files.add(file.toString() + "--->" + file.getFileName()));
		return files;

	}
}
