using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using static Amazon.S3.Util.S3EventNotification;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AWSLambdaS3
{
    public class Function
    {
        #region Variables

        private const string PATH = @"C:\Users\382420\Downloads\",
                             FILEFILTER = @"[^\s]+(\.(?i)(csv|xls|xlsx))$";

        #endregion

        #region Properties

        IAmazonS3 S3Client { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        #endregion

        #region Functions

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="s3Event"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task ProcessFile(S3Event s3Event, ILambdaContext context)
        {
            var time = new Stopwatch();
            time.Start();
            await this.TryForLoop(s3Event, context);
            time.Stop();
            var time1 = time.ElapsedMilliseconds;

            time.Restart();
            this.TryParallelForLoop(s3Event, context);
            time.Stop();
            var time2 = time.ElapsedMilliseconds;

            time.Restart();
            await this.TryForEachLoop(s3Event, context);
            time.Stop();
            var time3 = time.ElapsedMilliseconds;

            context.Logger.LogLine($"1: {time1}, 2: {time2}, 3: {time3}");
        }

        private async Task TryForLoop(S3Event s3Event, ILambdaContext context)
        {
            try
            {
                if (s3Event?.Records?.Count > 0)
                {
                    for (int index = 0; index < s3Event.Records.Count; index++)
                    {
                        try
                        {
                            GetObjectResponse theFile = await this.S3Client.GetObjectAsync(s3Event.Records[index].S3.Bucket.Name, s3Event.Records[index].S3.Object.Key);
                            using (var stream = new StreamReader(theFile.ResponseStream, Encoding.UTF8))
                            {
                                //Do something
                            }

                            //Deletes the object after processing it.
                            await this.S3Client.DeleteObjectAsync(s3Event.Records[index].S3.Bucket.Name, s3Event.Records[index].S3.Object.Key);
                        }
                        catch (Exception x)
                        {
                            context.Logger.LogLine($"Error: {context.FunctionName} - Object Key: {s3Event.Records[index].S3.Object.Key}, Bucket Name: {s3Event.Records[index].S3.Bucket.Name}, Message: {x.ToString()}");
                        }
                    }

                    string[] fileNames = Directory.GetFiles(Function.PATH).Where(path => Regex.Match(path, Function.FILEFILTER).Success).ToArray();
                    for (int index = 0; index < fileNames.Length; index++)
                    {
                        using (var stream = new StreamReader(fileNames[index], Encoding.UTF8))
                        {
                            //Do something
                        }
                    }
                }
            }
            catch (Exception x)
            {
                context.Logger.LogLine($"Error: {context.FunctionName} - Message: {x.ToString()}");
                throw;
            }
        }

        private void TryParallelForLoop(S3Event s3Event, ILambdaContext context)
        {
            try
            {
                if (s3Event?.Records?.Count > 0)
                {
                    Parallel.For(0, s3Event.Records.Count, index =>
                    {
                        try
                        {
                            Task<GetObjectResponse> task = this.S3Client.GetObjectAsync(s3Event.Records[index].S3.Bucket.Name, s3Event.Records[index].S3.Object.Key);
                            using (var stream = new StreamReader(task.Result.ResponseStream, Encoding.UTF8))
                            {
                                //Do something
                            }

                            //Deletes the object after processing it.
                            this.S3Client.DeleteObjectAsync(task.Result.BucketName, task.Result.Key);
                        }
                        catch (Exception x)
                        {
                            context.Logger.LogLine($"Error: {context.FunctionName} - Object Key: {s3Event.Records[index].S3.Object.Key}, Bucket Name: {s3Event.Records[index].S3.Bucket.Name}, Message: {x.ToString()}");
                        }
                    });

                    string[] fileNames = Directory.GetFiles(Function.PATH).Where(path => Regex.Match(path, Function.FILEFILTER).Success).ToArray();
                    Parallel.For(0, fileNames.Length, index =>
                    {
                        using (var stream = new StreamReader(fileNames[index], Encoding.UTF8))
                        {
                            //Do something
                        }
                    });
                }
            }
            catch (Exception x)
            {
                context.Logger.LogLine($"Error: {context.FunctionName} - Message: {x.ToString()}");
                throw;
            }
        }

        private async Task TryForEachLoop(S3Event s3Event, ILambdaContext context)
        {
            try
            {
                if (s3Event?.Records?.Count > 0)
                {
                    foreach (S3EventNotificationRecord record in s3Event.Records)
                    {
                        try
                        {
                            GetObjectResponse theFile = await this.S3Client.GetObjectAsync(record.S3.Bucket.Name, record.S3.Object.Key);
                            using (var stream = new StreamReader(theFile.ResponseStream, Encoding.UTF8))
                            {
                                //Do something
                            }

                            //Deletes the object after processing it.
                            await this.S3Client.DeleteObjectAsync(record.S3.Bucket.Name, record.S3.Object.Key);
                        }
                        catch (Exception x)
                        {
                            context.Logger.LogLine($"Error: {context.FunctionName} - Object Key: {record.S3.Object.Key}, Bucket Name: {record.S3.Bucket.Name}, Message: {x.ToString()}");
                        }
                    }

                    foreach (string filePath in Directory.GetFiles(Function.PATH).Where(path => Regex.Match(path, Function.FILEFILTER).Success))
                    {
                        using (var stream = new StreamReader(filePath, Encoding.UTF8))
                        {
                            //Do something
                        }
                    }
                }
            }
            catch (Exception x)
            {
                context.Logger.LogLine($"Error: {context.FunctionName} - Message: {x.ToString()}");
                throw;
            }
        }

        #endregion
    }
}