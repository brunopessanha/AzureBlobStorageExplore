using System.Runtime.CompilerServices;
using System;
using System.Linq;
using Azure.Storage.Blobs;
using System.IO;
using System.Text;
using CommandLine;
using System.Collections.Generic;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

namespace BlobStorageExplorer {
    class Program {

        static String connectionString = "[Connection String]";

        static bool exit = false;

        static Options currentOptions = new Options();

        public class Options {
            [Option('l', "list", Required = false, HelpText = "List blob items in the container.")]
            public bool List { get; set; }

            [Option('s', "switch", Required = false, HelpText = "Switch to a container listed in 'c' and then lists the blob items in the container.")]
            public bool Switch { get; set; }

            [Option('e', "exit", Required = false, HelpText = "Exit this program")]
            public bool Exit { get; set; }

            [Option('c', "container", Required = false, HelpText = "The name of the container.")]
            public String Container { get; set; }

            [Option('d', "directory", Required = false, HelpText = "The name of the container.")]
            public String Directory { get; set; }
            
            [Option('b', "blob lease", Required = false, HelpText = "The name of the blob you wish to get lease for.")]
            public String Blob { get; set; }

            [Option('t', "blob lease time", Required = false, HelpText = "Lease time in minutes.")]
            public int? LeaseTime { get; set; }
        }
        
        static void Main(string[] args) {

            while(!exit) {
                var currentOptions = Parser.Default.ParseArguments<Options>(args)
                    .WithParsed(RunOptions)
                    .WithNotParsed(HandleParseError);


                args = Console.ReadLine().Split(" ");
            }

        }

        static void RunOptions(Options opts) {

            if (opts.Exit) {
                exit = true;
                return;
            }

            Console.WriteLine($"--- CURRENT CONTAINER: {opts.Container ?? currentOptions.Container ?? "<NO CONTAINER>"} ---");

            if (opts.Container != null || currentOptions != null) {
                currentOptions.Container = opts.Container ?? currentOptions.Container;
                var containerClient = new BlobContainerClient(connectionString, currentOptions.Container );

                if (opts.List) {
                    ListBlobs(containerClient);
                }

                if (opts.Switch) {
                    ShowNewContainer(containerClient);
                    ListBlobs(containerClient);
                }

                if (opts.Directory != null) {
                    CreateNewDirectory(containerClient, opts.Directory);
                }

                
                if (opts.Blob != null) {
                    GetLeaseForBlob(containerClient, opts.Blob, opts.LeaseTime ?? 1);
                }
            }
        }

        static void ShowNewContainer(BlobContainerClient containerClient) {

            var properties = containerClient.GetProperties();
            Console.WriteLine($"\tPublic Access Level: {properties.Value.PublicAccess.Value}");

            properties.Value.Metadata.ToList().ForEach(m => {
                Console.WriteLine($"\tMetadata {m.Key} = {m.Value}");
            });

            var policy = containerClient.GetAccessPolicy().Value;
            Console.WriteLine($"\tAccess Policy - Public Access: {policy.BlobPublicAccess} \t ETag: {policy.ETag}");

            policy.SignedIdentifiers.ToList().ForEach(s => {
                Console.WriteLine($"\tId: {s.Id}");
                Console.WriteLine($"\tStarts On: {s.AccessPolicy.PolicyStartsOn}\t Expires On: {s.AccessPolicy.PolicyExpiresOn}");
                Console.WriteLine($"\tPermissions: {s.AccessPolicy.Permissions}");
            });
        }

        static void ListBlobs(BlobContainerClient containerClient) {

            var blobs = containerClient.GetBlobs();
            Console.WriteLine($"There are {blobs.Count()} blobs in the container {containerClient.Name}");

            blobs.ToList().ForEach(blob => {
                Console.WriteLine($"Blob {blob.Name} \t {blob.Properties.AccessTier} \t {blob.Properties.BlobType} \t {blob.Properties.LastModified}");
            });
        }

        static void CreateNewDirectory(BlobContainerClient containerClient, String directoryName) {
            containerClient.UploadBlob(directoryName, new MemoryStream()); // not really a directory
        }

        static void GetLeaseForBlob(BlobContainerClient containerClient, String blobName, int minutes) {

            Console.WriteLine($"\tAcquiring Lease for blob {blobName} in container {containerClient.Name} for {minutes} minutes...");

            var blobLeaseClient = new BlobLeaseClient(containerClient.GetBlobClient(blobName));
            var lease = blobLeaseClient.Acquire(new TimeSpan(0, minutes, 0));

            Console.WriteLine($"\tLease acquired: Id = {lease.Value.LeaseId} \t Time = {lease.Value.LeaseTime}");
        }

        static void HandleParseError(IEnumerable<Error> errs) {
        //handle errors
        }
    }
}
