const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB.DocumentClient();

/*

  Author:Jayant Patidar B00934519

  Description : Lambda to be triggered when there is anew file in s3 bucket. It read the JSON content from each file and updated the DynamoDB database table with the named entities as keys and their corresponding counts as values.

  References used : 

[1] Amazon Web Services, Inc., “AWS Lambda Documentation,” aws.amazon.com, 2021. [Online]. Available: https://docs.aws.amazon.com/lambda/index.html (accessed Jul. 20, 2023).

[2] Amazon Web Services, “What is Amazon S3? - Amazon Simple Storage Service,” docs.aws.amazon.com, 2023. [Online].  Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html (accessed Jul. 20, 2023).

[3] “What Is Amazon DynamoDB? - Amazon DynamoDB,” docs.aws.amazon.com. [Online]. Available: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html (accessed Jul. 20, 2023).

[4] “Getting started in Node.js - AWS SDK for JavaScript,” docs.aws.amazon.com. [Online]. Available: https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started-nodejs.html (accessed Jul. 20, 2023).

[5] AWS, “What is Amazon CloudWatch? - Amazon CloudWatch,” Amazon.com, 2019. [Online]. Available: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html (accessed Jul. 20, 2023).

*/

// Lambda function to update DynamoDB with named entities and their counts
exports.handler = async (event) => {
  // Extract the bucket name and object key from the S3 event
  const bucketName = event.Records[0].s3.bucket.name;
  const key = event.Records[0].s3.object.key;
  //console.log("*****" + bucketName + "##" + key + "$$");

  // Get the file name without the extension to use as the ID for DynamoDB
  const dotIndex = key.lastIndexOf(".");
  const fileName = key.split("/").pop().substring(0, dotIndex);
  console.log("file_name done");

  try {
    // Retrieve the content of the file from the S3 bucket
    const data = await s3.getObject({ Bucket: bucketName, Key: key }).promise();
    const fileContent = data.Body.toString();

    // Parse the file content as JSON to get the named entities and their counts
    const namedEntitiesObject = JSON.parse(fileContent);

    // Update the DynamoDB table with the named entities and their counts
    for (const [entity, value] of Object.entries(namedEntitiesObject)) {
      await updateDynamoDBTable(entity, value);
    }

    // Return a success response with a status code of 200 and a message
    return {
      statusCode: 200,
      body: JSON.stringify("DynamoDB updated with named entities."),
    };
  } catch (error) {
    // If there is an error during the DynamoDB update process, log the error and re-throw it
    console.error("Error updating DynamoDB:", error);
    throw error;
  }
};

// Function to update the DynamoDB table with named entities and their counts
async function updateDynamoDBTable(entity, data) {
  console.log(data);
  const tableName = "names-table";
  try {
    for (const key in data) {
      const params = {
        TableName: tableName,
        Key: { entity: key },
        UpdateExpression: "ADD #value :value",
        ExpressionAttributeNames: { "#value": "value" },
        ExpressionAttributeValues: { ":value": data[key] },
      };

      await dynamodb
        .update(params)
        .promise()
        .catch(async (err) => {
          if (
            err.code === "ValidationException" &&
            err.message.includes("attribute does not exist")
          ) {
            // If the key doesn't exist, insert the new key-value pair
            params.UpdateExpression = "SET #value = :value";
            await dynamodb.put(params).promise();
          }
        });
    }
    console.log("Data inserted/updated successfully.");
  } catch (error) {
    console.error("Error putting data into DynamoDB:", error);
  }
}
