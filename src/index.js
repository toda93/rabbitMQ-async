import amqp from 'amqplib';

class RabbitMQAsync {
    constructor(config) {
        this.connected = false;
        this.config = config;
        this.connect();
    }

    async connect() {
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        this.client = await amqp.connect(`amqp://${this.config.host}`, opt);

        this.client.on('error', (err) => {
            this._alert('error', 'MQ Error' + err);
        });

        this.client.on('close', () => {
            this._alert('close', 'MQ close');
        });

        this._alert('connect', 'MQ connect');
    }




    async close() {
        this.isConnected = false;
        this.client.close();
    }

    async send(queue, msg) {
        let channel = await this.client.createChannel();
        await channel.assertQueue(queue, {
            durable: true
        });
        await channel.sendToQueue(queue, Buffer.from(msg), {
            persistent: true
        });
        console.log(`Send ${queue}, ${msg}`);
    }

    async receiving(queue, cb) {
        let channel = await this.client.createChannel();
        await channel.assertQueue(queue, {
            durable: true
        });
        await channel.consume(queue, cb, {
            noAck: true
        });
    }

    setAlertCallback(callback) {
        this.alertCallback = callback;
    }

    _alert(status, msg) {
        if (typeof this.alertCallback === 'function') {
            this.alertCallback(status, msg);
        } else {
            console.info(status, msg);
        }
    }
}

export default RabbitMQAsync;