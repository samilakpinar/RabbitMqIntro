using RabbitMQ.Client;
using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace UdemyRabbitMQ.publisher
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //Ilk olarak rabbitMQya bağlanmak için bir connection factory isminde bir class oluşturulması gereklidir.
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://fdhhqcpb:nJ-oHCRlLFfFfsCcOqZ3NxJHFDn_x40n@chimpanzee.rmq.cloudamqp.com/fdhhqcpb");

            //Bir bağlantının acılması
            using var connection = factory.CreateConnection();

            //RabbitMqya bir kanal üzerinden bağlanılacaktır. Bu kanal üzerinden rabbitMq ile haberleşilebilir.
            var channel = connection.CreateModel();

            //Fanout Exchange yapısının tanımlanması:
            //durable:fiziksel olarak kaydedilsin. uygualam restart yapıldığında bu exchange kaybolmasın.
            channel.ExchangeDeclare("logs-fanout",durable:true,type:ExchangeType.Fanout);

            //NOT:uygulamayı 1 kere ayağa kaldırıldığında 50 kere mesaj gitsin.
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                //mesajın oluşturulması:
                //rabbitMqya mesajları byte dizisi olarak gönderilir bu şekilde istenilen her şey gönderilir. pdf, image vb.
                string message = $"log {x}";

                //mesajın byte çevrilmesi:
                var messageBody = Encoding.UTF8.GetBytes(message);

                //kuyruğa gönderilmesi:
                //arada exchange yapısı verildi.
                //Kuyruk boş bırakıldı.
                channel.BasicPublish("logs-fanout", "", null, messageBody);

                Console.WriteLine($"Mesaj gönderilmiştir : {message}");
            });
                      

            Console.ReadLine();
        }
    }
}
