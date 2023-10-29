using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace UdemyRabbitMQ.subscriber
{
    internal class Program
    {
        static void Main(string[] args)
        {

            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://fdhhqcpb:nJ-oHCRlLFfFfsCcOqZ3NxJHFDn_x40n@chimpanzee.rmq.cloudamqp.com/fdhhqcpb");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();


            //random olarak bir kuyruk yapısı ver.
            var randomQueueName = channel.QueueDeclare().QueueName;


            //kuyruğun tekrar silinmesi için bind edildi.
            //geçici kuyruklardır.
            channel.QueueBind(randomQueueName, "logs-fanout", "", null);
            
            //Projeye eklenen 2. kısım: projeye mesajları kaç kaç gönderilecek.
            //herbir subscribera kaç mesaj geleceğinin bilgilendirilmesi.
            // 0-> bana herhangi bir boyuttaki mesajı gönderebilirsin demektedir.
            //kaç kaç mesajlar gelsin. her bir subscriber a 1 er tane gelsin.
            //Eğer iki tane subscriber var ise tek bir seferde 1 mesaj, 1.subsribera 1 mesaj, 2. subscribera.
            //Eğer 10 mesaj varsa subscriber 1 tane mesajı alacak geri kalan 9 tanesi memoryde bekleyecektir.
            channel.BasicQos(0, 1,false);

            //NOT: channel.BasicQos(0, 6, true);
            //böyle bir durumda eğer iki tane subscriber varsa 3 3 olacak şekilde iki subsriber a dağıtır.
            //Eğer 3 subscriber var sa bu sefer 2 2 2 şekilde mesajları iletir.
            //Eğer false yapılırsa -> tek bir seferde 1. subscribera 6 2. subscribera 6 şekilde gönderme işlemi yapacaktır.


            var consumer = new EventingBasicConsumer(channel);

            //autoAck parametresi: true verilirse -> subscriber mesajı aldığında direk olarak kuyruktan silecektir.
            //false verilirse -> rabbitmq subsrcibera bir mesaj gönderdiğinde bu mesajı silmez mesaj doğru işlenirse silme işlemi istenir.
            //(Projeye iki özellik ekledik. 1- false parametresi ile okuduğu mesajı hemen silme dedik. ben sana haber vereceğim dedim. sileceğini haber verme kısmı aşağıda.)
            channel.BasicConsume(randomQueueName, false, consumer);

            Console.WriteLine("Loglar dinleniyor.");
            //Event üzerinden dinleme:
            //rabbitmq subscribera bir mesaj gönderdiğinde buradaki event fırlıyor
            //lamda ile kullanımı
            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                //gelen byte dizisini texte çevirmek için
                var message = Encoding.UTF8.GetString(e.Body.ToArray());


                //mesajlar çok hızlı işlendiğinden 1.5 saniyelik geçikme verilecektir.
                Thread.Sleep(1500);

                Console.WriteLine("gelen mesaj:"+ message);

                //Burada mesajı silmesi için haber verdik.
                //bu mesaj işlendikten sonra, iligli mesajı artık silebilirsin.
                //true denilirse: o anda memoryde işlenmiş ama rabbitMqya gitmemiş başka mesajlarda varsa onun bilgilerini de rabbitmq ya haberdar eder.
                //false: tek mesajı işlediğimiz için verildi.
                //rabbitmwya bilgirme işlemini gerçekleştirecektir.
                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }

        
    }
}
