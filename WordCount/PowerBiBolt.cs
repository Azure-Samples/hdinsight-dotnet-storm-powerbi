using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using System.Net;
using PowerBI.Api.Client;

namespace WordCount
{
    public class PowerBiBolt : ISCPBolt
    {
        Context context;

        private string datasetName { get; set; }
        private string datasetId;

        public PowerBiBolt(Context context)
        {
            this.context = context;

            datasetName = Properties.Settings.Default.DatasetName;

            //Get the dataset info (ID). Create the dataset if it doesn't exist
            //NOTE: for a production scenario, you would want to pre-create
            //the dataset, since creating multiple bolts would (potentiall) 
            //result in creating multiple datasets with the same name
            GetDataset();

            //Declare Input and Output schemas
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            //The input contains a tuple containing a string field (the word,) and the count
            inputSchema.Add("default", new List<Type>() { typeof(string), typeof(int) });
            this.context.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, null));
        }

        public void Execute(SCPTuple tuple)
        {
            PowerBIClient.Do(api =>
            {
                var isObjectInsert = api.Insert(datasetId, new Data.WordCount
                {
                    Word = tuple.GetString(0),
                    Count = tuple.GetInteger(1)
                });
            }); 
        }

        // Get a new instance
        public static PowerBiBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new PowerBiBolt(ctx);
        }

        void GetDataset()
        {
            PowerBIClient.Do(api =>
            {
                //Create if not exist
                bool datasetExist = api.IsDatasetExist(datasetName);
                if(!datasetExist)
                {
                    bool created = api.CreateDataset(datasetName,false, typeof(Data.WordCount));
                }
                //Set the ID to use for the dataset
                datasetId = api.GetDatasetId(datasetName);
            });
        }
    }
}