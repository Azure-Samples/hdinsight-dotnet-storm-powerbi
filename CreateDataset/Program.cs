using PowerBI.Api.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CreateDataset
{
    class Program
    {
        static void Main(string[] args)
        {
            string datasetName = "Words";

            PowerBIClient.Do(api =>
            {
                //Create if not exist
                bool datasetExist = api.IsDatasetExist(datasetName);
                if (!datasetExist)
                {
                    Console.WriteLine("Creating dataset.");
                    bool created = api.CreateDataset(datasetName, false, typeof(Data.WordCount));
                    if(created)
                    {
                        Console.WriteLine("Successfully created.");
                    }
                    else
                    {
                        Console.WriteLine("Unable to create dataset.");
                    }
                }
                else
                {
                    Console.WriteLine("Dataset already exists.");
                }
            });
            Console.Write("Press any key to continue.");
            Console.ReadKey();
        }
    }
}
