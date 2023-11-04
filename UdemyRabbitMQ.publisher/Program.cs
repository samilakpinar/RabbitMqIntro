using RabbitMQ.Client;
using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace UdemyRabbitMQ.publisher
{
    internal class Program
    {
        public enum LogNames
        {
            Critical=1,
            Error=2,
            Warning=3,
            Info=4
        }
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
            channel.ExchangeDeclare("logs-direct",durable:true,type:ExchangeType.Direct);

            Enum.GetNames(typeof(LogNames)).ToList().ForEach(x =>
            {
                var rootKey = $"root-{x}";

                //Her dönüldüğünde bir kuyruk oluşturulur.
                var queueName = $"direct-queue-{x}";
                channel.QueueDeclare(queueName, true, false, false);

                channel.QueueBind(queueName, "logs-direct", rootKey,null);
            });


            //NOT:uygulamayı 1 kere ayağa kaldırıldığında 50 kere mesaj gitsin.
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {

                LogNames log = (LogNames)new Random().Next(1, 5);

                //mesajın oluşturulması:
                //rabbitMqya mesajları byte dizisi olarak gönderilir bu şekilde istenilen her şey gönderilir. pdf, image vb.
                string message = $"log-type: {log}";

                //mesajın byte çevrilmesi:
                var messageBody = Encoding.UTF8.GetBytes(message);

                var rootKey = $"root-{log}";

                //kuyruğa gönderilmesi:
                //arada exchange yapısı verildi.
                //Kuyruk boş bırakıldı.
                channel.BasicPublish("logs-direct", rootKey, null, messageBody);

                Console.WriteLine($"Log gönderilmiştir : {message}");
            });
                      

            Console.ReadLine();
        }
    }
}
