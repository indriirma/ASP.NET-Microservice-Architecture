using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using PostService.Data;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks; 

namespace PostService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            ListenForIntegrationEvents();
            CreateHostBuilder(args).Build().Run();
        }

        private static void ListenForIntegrationEvents()
        {
            var factory = new ConnectionFactory { HostName="localhost"};
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "user.postservice",
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
            var consumer = new EventingBasicConsumer(channel); 
            consumer.Received += (model, ea) =>
            {
                try 
                { 
                    var ContextOptions = new DbContextOptionsBuilder<PostServiceContext>()
                    .UseSqlServer("Server=DESKTOP-KVQ0RKB\\SQLEXPRESS; Database=Micro2Db; Trusted_Connection=True; MultipleActiveResultSets=true")
                    .Options;
                    var dbContext = new PostServiceContext(ContextOptions);
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine("[x] Received {0}",message);
                    var data = JObject.Parse(message);
                    var type = ea.RoutingKey;
                    if(type == "user.add")
                    {
                        dbContext.User.Add(new Entities.User() {
                            ID = data["id"].Value<int>(),
                            Name = data["name"].Value<string>(),
                            Version = data["version"].Value<int>()
                        }) ;
                        dbContext.SaveChanges();
                    }
                    else if(type=="user.update")
                    {
                        int newVersion = data["version"].Value<int>();
                        var user = dbContext.User.First(a => a.ID == data["id"].Value<int>());
                        if(user.Version>=newVersion)
                        {
                            Console.WriteLine("Ignoring old/duplicate entity");
                        }
                        else
                        {
                            user.Name = data["newname"].Value<string>();
                            user.Version = newVersion;
                            dbContext.SaveChanges();
                        } 
                    }
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error Message : " + ex.Message);
                }
                
            };
            channel.BasicConsume(queue: "user.postservice",
                       autoAck: false,
                       consumer: consumer);

        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
