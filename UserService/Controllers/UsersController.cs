using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UserService.Data;
using UserService.Entities;

namespace UserService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        private readonly UserServiceContext _context;
        private readonly IntegrationEventSenderService _integrationEventSenderService;
        public UsersController(UserServiceContext context, IntegrationEventSenderService integrationEventSenderService)
        {
            _context = context;
            _integrationEventSenderService = integrationEventSenderService;
        }
        [HttpGet]
        public async Task<ActionResult<IEnumerable<User>>>GetUser()
        {
            return await _context.User.ToListAsync();
        }
        private static void PublishToMessageQueue(string integrationEvent, string eventData)
        {
            var factory = new ConnectionFactory { HostName="localhost"};
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "user.postservice",
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
            var body = Encoding.UTF8.GetBytes(eventData);
            var routingKey = integrationEvent;
            channel.BasicPublish(exchange: "user",
                routingKey:routingKey,
                basicProperties:null,
                body:body
                );
            Console.WriteLine("Data sender");
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> PutUser(int id, User user)
        {
            using var transaction = _context.Database.BeginTransaction();
            _context.Entry(user).State = EntityState.Modified;
            await _context.SaveChangesAsync();

            var integrationEventData = JsonConvert.SerializeObject(new
            {
                id = user.ID,
                newname = user.Name
            });
            _context.IntegrationEventOutBox.Add(
                new IntegrationEvent()
                {
                    Event = "user.update",
                    Data = integrationEventData
                } );
            _context.SaveChanges();
            transaction.Commit();
            //PublishToMessageQueue("user.update", integrationEventData);
            _integrationEventSenderService.StartPublishingOutstandingIntegrationEvents();
            return NoContent();
        }

        [HttpPost]
        public async Task<ActionResult<User>> PostUser(User user)
        {
            user.Version = 1;
            using var transaction = _context.Database.BeginTransaction();
            _context.User.Add(user);
            _context.SaveChanges();

            var integrationEventData = JsonConvert.SerializeObject(new
            {
                id = user.ID,
                name = user.Name,
                version = user.Version
            });
            _context.IntegrationEventOutBox.Add(
                new IntegrationEvent()
                {
                    Event = "user.add",
                    Data = integrationEventData
                });
            _context.SaveChanges();
            transaction.Commit();
            //PublishToMessageQueue("user.add", integrationEventData);
            _integrationEventSenderService.StartPublishingOutstandingIntegrationEvents();
            return CreatedAtAction("GetUser", new { id = user.ID }, user);
        }
    }
}
