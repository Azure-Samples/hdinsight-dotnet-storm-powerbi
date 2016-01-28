using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace WordCount
{
    public class Spout : ISCPSpout
    {
        private Context ctx;
        private Random r = new Random();
        string[] sentences = new string[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        };

        // Constructor
        public Spout(Context ctx)
        {
            this.ctx = ctx;

            // Declare Output schema
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            // The schema for the default output stream is
            // a tuple that contains a string field
            outputSchema.Add("default", new List<Type>() { typeof(string) });
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));
        }

        // Get a new instance of the spout
        public static Spout Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new Spout(ctx);
        }

        public void NextTuple(Dictionary<string, Object> parms)
        {
            // The sentence to be emitted
            string sentence;

            // Get a random sentence
            sentence = sentences[r.Next(0, sentences.Length - 1)];
            // Emit it
            this.ctx.Emit(new Values(sentence));
        }

        public void Ack(long seqId, Dictionary<string, Object> parms)
        {

        }

        public void Fail(long seqId, Dictionary<string, Object> parms)
        {

        }
    }
}