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

        this.client.on('connect', () => {
            this.connected = true;
            this._alert('connect', 'MQ connected');
        });

        this.client.on('end', () => {
            this._alert('end', 'MQ end');
        });

        this.client.on('error', (err) => {
            this._alert('error', 'MQ Error' + err);
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