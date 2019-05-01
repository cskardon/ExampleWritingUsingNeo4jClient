using System;

namespace EmailParser
{
    using System.Collections.Generic;
    using Neo4jClient;

    class Program
    {
        static void Main(string[] args)
        {
            //New Bolt Graph Client
            var graphClient = new BoltGraphClient("bolt://localhost:7687/", "neo4j", "neo");
            graphClient.Connect();

            //Store the data from the DataProvider
            StoreData(graphClient, GetDataProvider());
            Console.WriteLine("Done!");
        }

        //This is the Cypher bit.
        //I've used 'nameof' to allow for some type safety - i.e. if someone changes the property name on a 'Person' the code will break, so it will be obvious. 
        static void StoreData(IGraphClient client, IDataProvider dataProvider)
        {
            foreach (var data in dataProvider.GetExchangeData())
            {
                client.Cypher
                    .Merge($"(from:{Person.Labels} {{Email: $frm.{nameof(Person.Email)} }})")
                    .OnCreate().Set($"from.Name = $frm.{nameof(Person.Name)}")
                    .Merge($"(e:{Email.Labels} {{Uuid: $data.{nameof(FromExchange.Id)}}})")
                    .Merge($"(from)-[:{RelationshipTypes.Sent}]->(e)")
                    .With("e")
                    .Unwind(data.To, "to")
                    .Merge($"(t:{Person.Labels} {{Email: to.{nameof(Person.Email)}}})")
                    .OnCreate().Set($"t.Name = to.{nameof(Person.Name)}")
                    .Merge($"(e)-[:{RelationshipTypes.Received}]->(t)")
                    .WithParams(new {data, frm = data.From})
                    .ExecuteWithoutResults();
            }
        }

        static IDataProvider GetDataProvider()
        {
            //Normally this would go to Exchange and get the data.
            return new TestDataProvider(100);
        }
    }

    // I use this to 'Type-safe' the types - to prevent accidents!
    public static class RelationshipTypes
    {
        public const string Sent = "SENT";
        public const string Received = "RECEIVED";
    }

    // To allow us to switch real / test data providers
    public interface IDataProvider
    {
        IEnumerable<FromExchange> GetExchangeData();
    }
    
    // A Test provider - randomly makes users and emails.
    public class TestDataProvider : IDataProvider
    {
        private readonly Random _random = new Random((int)DateTime.Now.Ticks);
        private readonly List<FromExchange> _data = new List<FromExchange>();

        public TestDataProvider(int count)
        {
            for (int i = 0; i < count; i++)
            {
                var fi = _random.Next(count);
                var ft = _random.Next(count);
                if (ft == fi) ft = _random.Next(count);
                var from = new Person {Email = $"user{fi}@testplace.com", Name = $"Test Person_{fi}"};
                var to = new Person {Email = $"user{ft}@testplace.com", Name = $"Test Person_{ft}"};
                _data.Add(new FromExchange{From = from, To = new List<Person>{to}, Id = Guid.NewGuid(), Subject = $"Subject {_random.Next(count)}"});
            }
        }

        public IEnumerable<FromExchange> GetExchangeData()
        {
            foreach (var t in _data)
                yield return t;
        }
    }


    /// <summary>
    /// Implement this one for actual data
    /// </summary>
    public class DataProvider : IDataProvider {

        public IEnumerable<FromExchange> GetExchangeData()
        {
            throw new NotImplementedException();
        }
    }

    //The output from the 'IDataProvider' GetExchangeData method. Obviously changes to this would mean changes to the Cypher etc.
    public class FromExchange
    {
        public Person From { get; set; }
        public IEnumerable<Person> To { get; set; }
        public IEnumerable<Person> CC { get; set; }
        
        public string Subject { get; set; }
        public Guid Id { get; set; }
    }

    #region What we're storing
    public class Email 
    {
        public const string Labels = "Email";
        public Guid Id { get; set; }
        public string Subject { get; set; }
       
    }

    public class Person
    {
        public const string Labels = "Person";
        public string Name { get; set; }
        public string Email { get; set; }
        
    }
    #endregion What we're storing
}
