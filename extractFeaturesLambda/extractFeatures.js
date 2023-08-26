const AWS = require("aws-sdk");
const s3 = new AWS.S3();

/*

  Author:Jayant Patidar B00934519

  Description : Lambda to be triggered when there is a new file in s3 bucket. It extractes named entities from the file content using a regular expression and created a JSON array of the entities.The resulting JSON content was uploaded to the "tagsb00934519" bucket with filenames like "001ne.txt" to identify each file's named entities uniquely.


  References used : 

[1] Amazon Web Services, Inc., “AWS Lambda Documentation,” aws.amazon.com, 2021. [Online]. Available: https://docs.aws.amazon.com/lambda/index.html (accessed Jul. 20, 2023).

[2] Amazon Web Services, “What is Amazon S3? - Amazon Simple Storage Service,” docs.aws.amazon.com, 2023. [Online].  Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html (accessed Jul. 20, 2023).

[3] “What Is Amazon DynamoDB? - Amazon DynamoDB,” docs.aws.amazon.com. [Online]. Available: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html (accessed Jul. 20, 2023).

[4] “Getting started in Node.js - AWS SDK for JavaScript,” docs.aws.amazon.com. [Online]. Available: https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started-nodejs.html (accessed Jul. 20, 2023).

[5] AWS, “What is Amazon CloudWatch? - Amazon CloudWatch,” Amazon.com, 2019. [Online]. Available: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html (accessed Jul. 20, 2023).

*/

exports.handler = async (event) => {
  // Extract the bucket name and object key from the S3 event
  const bucketName = event.Records[0].s3.bucket.name;
  const key = event.Records[0].s3.object.key;

  //console.log("*****" + event.bucketName + "##" + key + "$$");

  try {
    // Retrieve the content of the file from the S3 bucket
    const data = await s3.getObject({ Bucket: bucketName, Key: key }).promise();
    const fileContent = data.Body.toString();

    // Extract named entities (words starting with capital letters) from the file content
    const namedEntities = fileContent.match(/\b[A-Z][a-zA-Z]*\b/g);

    // Create an object to store the named entities as keys with a value of 1 (arbitrary, can be any value)
    const namedEntitiesObject = {};
    namedEntities.forEach((entity) => {
      namedEntitiesObject[entity] = 1;
    });

    // Get the file base name without the extension to create the JSON key
    const dotIndex = key.lastIndexOf(".");
    const fileBaseName = key.substring(0, dotIndex);

    // Create a JSON object with the named entities and their counts and convert it to a string
    const jsonContent = JSON.stringify({
      [`${fileBaseName}ne`]: namedEntitiesObject,
    });

    // Upload the JSON content to a new S3 bucket named 'tagsb00934519' with the filename `${fileBaseName}ne.txt`
    await s3
      .upload({
        Bucket: "tagsb00934519",
        Key: `${fileBaseName}ne.txt`,
        Body: jsonContent,
      })
      .promise();

    // Return a success response with a status code of 200 and a message
    return {
      statusCode: 200,
      body: JSON.stringify(
        "Named entities extracted and saved to TagsB00934519 bucket."
      ),
    };
  } catch (error) {
    // If there is an error during the extraction and upload process, log the error and re-throw it
    console.error("Error extracting named entities:", error);
    throw error;
  }
};
