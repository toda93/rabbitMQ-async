import amqp from 'amqplib';

class RabbitMQAsync {
    constructor(config) {
        this.connected = false;
        this.config = config;
        this.connect();
    }

    async connect() {
        this._alert('connecting', this.config);
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        try {
            this.client = await amqp.connect(`amqp://${this.config.host}`, opt);
            this.connected = true;
            this._alert('connect', 'MQ connected');

            this.client.on('error', (err) => {
                this._alert('error', 'MQ Error' + err);
                const reconnect = this.connect;
                setTimeout(reconnect, 10000);
            });

            this.client.on('close', () => {
                this.connected = false;
                this._alert('close', 'MQ closed');
                const reconnect = this.connect;
                setTimeout(reconnect, 10000);
            });
        } catch (err) {
            this.connected = false;
            this._alert('error', 'MQ connect' + err);
            const reconnect = this.connect;
            setTimeout(reconnect, 10000);
        }
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