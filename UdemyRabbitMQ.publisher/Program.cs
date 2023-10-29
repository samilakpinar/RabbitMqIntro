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

            //Queue yapısının oluşturulması
            //durable yapısı (ikinci parametre) true olursa kuyruklar fiziksel olarak kaydedilir rabbitMQya
            //restart atılsa bile kuyruklar hiç bir zaman kaybolmaz  
            //false ise memoryde olur.
            //exclusive parametresi: true ise bu kuyruğa sadece yukarıda oluşturulan channel üzerinden bağlanmış olacaktır.
            //false durumunda buradaki kuyruğa subscriber tarafında farklı bir channel üzerinden bağlanılacaktır
            //autoDelete parametresi: kuyruğa bağlı olan son subscriber da bağlantısını kopatırsa kuyruğu otomatik olarak siler. 
            //false -> otomatik silinmesin anlamındadır.
            channel.QueueDeclare("hello-queue",true,false,false);

            //NOT:uygulamayı 1 kere ayağa kaldırıldığında 50 kere mesaj gitsin.
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                //mesajın oluşturulması:
                //rabbitMqya mesajları byte dizisi olarak gönderilir bu şekilde istenilen her şey gönderilir. pdf, image vb.
                string message = $"message {x}";

                //mesajın byte çevrilmesi:
                var messageBody = Encoding.UTF8.GetBytes(message);

                //kuyruğa gönderilmesi:
                //arada exchange kullanılmadığı için string.Empty verilir.
                channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);

                Console.WriteLine($"Mesaj gönderilmiştir : {message}");
            });
                      

            Console.ReadLine();
        }
    }
}
