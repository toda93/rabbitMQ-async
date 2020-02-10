import amqp from 'amqplib';

class RabbitMQAsync {
    constructor(config) {
        this.config = config;
    }

    async connect() {
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        this.connection = await amqp.connect(`amqp://${this.config.host}`, opt);
        this.isConnected = true;


        this.connection.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
    }




    async close() {
        this.isConnected = false;
        this.connection.close();
    }

    async send(queue, msg) {
        let channel = await this.connection.createChannel();
        await channel.assertQueue(queue, {
            durable: true
        });
        await channel.sendToQueue(queue, Buffer.from(msg), {
            persistent: true
        });
        console.log(`Send ${queue}, ${msg}`);
    }

    async receiving(queue, cb) {
        let channel = await this.connection.createChannel();
        await channel.assertQueue(queue, {
            durable: true
        });
        await channel.consume(queue, cb, {
            noAck: true
        });
    }
}

export default RabbitMQAsync;