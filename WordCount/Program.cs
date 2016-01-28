using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace WordCount
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        static void Main(string[] args)
        {
            Console.WriteLine("No local test - press any key to continue");
           
            Console.ReadKey();
        }

        public ITopologyBuilder GetTopologyBuilder()
        {
            // Create a new topology named 'WordCount'
            TopologyBuilder topologyBuilder = new TopologyBuilder("WordCount");

            // Add the spout to the topology.
            // Name the component 'sentences'
            // Name the field that is emitted as 'sentence'
            topologyBuilder.SetSpout(
                "sentences",
                Spout.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"sentence"}}
                },
                1);
            // Add the splitter bolt to the topology.
            // Name the component 'splitter'
            // Name the field that is emitted 'word'
            // Use suffleGrouping to distribute incoming tuples
            //   from the 'sentences' spout across instances
            //   of the splitter
            topologyBuilder.SetBolt(
                "splitter",
                Splitter.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"word"}}
                },
                1).shuffleGrouping("sentences");

            // Add the counter bolt to the topology.
            // Name the component 'counter'
            // Name the fields that are emitted 'word' and 'count'
            // Use fieldsGrouping to ensure that tuples are routed
            //   to counter instances based on the contents of field
            //   position 0 (the word). This could also have been 
            //   List<string>(){"word"}.
            //   This ensures that the word 'jumped', for example, will always
            //   go to the same instance
            topologyBuilder.SetBolt(
                "counter",
                Counter.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"word", "count"}}
                },
                1).fieldsGrouping("splitter", new List<int>() { 0 });

            //NOTE: This tends to work with parallelism hint set to 1.
            //If you increase this, you should pre-create the dataset
            //used by the bolt, as multiple instances of the bolt all
            //trying to create a new dataset will sometimes create
            //multiple datasets in Power BI, all with the same name.
            topologyBuilder.SetBolt(
                "PowerBI",
                PowerBiBolt.Get,
                new Dictionary<string, List<string>>(),
                1).fieldsGrouping("counter", new List<int>() { 0 });

            // Add topology config
            topologyBuilder.SetTopologyConfig(new Dictionary<string, string>()
            {
                {"topology.kryo.register","[\"[B\"]"}
            });

            return topologyBuilder;
        }
    }
}

