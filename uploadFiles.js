const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");

/*

  Author:Jayant Patidar B00934519

  Description : The code to establish connection with AWS and upload files in S3 bucket with 100 ms delay.

  References used : 


[1] Amazon Web Services, “What is Amazon S3? - Amazon Simple Storage Service,” docs.aws.amazon.com, 2023. [Online].  Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html (accessed Jul. 20, 2023).

[2] “Getting started in Node.js - AWS SDK for JavaScript,” docs.aws.amazon.com. [Online]. Available: https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started-nodejs.html (accessed Jul. 20, 2023).

*/

// Configure AWS credentials and region
AWS.config.update({
  region: "",
  accessKeyId: "",
  secretAccessKey: "",
  sessionToken: "",
});

const s3 = new AWS.S3();

const techFolderPath = "./tech";

// Function to upload a file to S3 bucket
const uploadFileToS3 = (bucketName, key, filePath) => {
  return new Promise((resolve, reject) => {
    const fileContent = fs.readFileSync(filePath);
    //console.log(bucketName);
    //console.log(key);
    //console.log(filePath);
    const params = {
      Bucket: bucketName,
      Key: key,
      Body: fileContent,
    };

    // Upload the file to S3 bucket
    s3.upload(params, (err, data) => {
      if (err) {
        console.error(`Error uploading file ${key}:`, err);
        reject(err);
      } else {
        console.log(`File ${key} uploaded successfully.`);
        resolve();
      }
    });
  });
};

// Function to upload files from the Tech folder to S3 bucket sequentially with a delay of 100 milliseconds
const uploadFilesSequentially = async () => {
  const files = fs.readdirSync(techFolderPath);

  // Iterate through each file and upload to S3
  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    //console.log(file);
    const filePath = path.join(techFolderPath, file);

    try {
      // Upload the file to S3 bucket
      await uploadFileToS3("sampledatab00934519", file, filePath);

      // Introduce a delay of 100 milliseconds before uploading the next file
      await delay(100);
    } catch (err) {
      console.error("Error during file upload:", err);
      break;
    }
  }
};

// Function to introduce a delay
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Call the function to upload files sequentially to S3 bucket
uploadFilesSequentially()
  .then(() => console.log("All files uploaded successfully."))
  .catch((err) => console.error("Error during file uploads:", err));
