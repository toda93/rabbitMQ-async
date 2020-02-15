import amqp from 'amqplib';


function timeout(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

class RabbitMQAsync {
    constructor(config) {
        this.connected = false;
        this.config = config;
        this.connect();
    }

    async connect() {
        this._alert('connecting', 'MQ connecting...');
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        try {
            this.client = await amqp.connect(`amqp://${this.config.host}`, opt);
            this.connected = true;
            this._alert('connect', 'MQ connected');

            this.client.on('error', (err) => {
                this.connected = false;
                this._alert('error', 'MQ Error' + err);
                this.reconnect();
            });

            this.client.on('close', () => {
                this.connected = false;
                this._alert('close', 'MQ closed');
                this.reconnect();
            });
        } catch (err) {
            this.connected = false;
            this._alert('error', 'MQ connect' + err);
            this.reconnect();
        }
    }
    async reconnect() {
        this._alert('reconnect', 'MQ try reconnect...');
        await timeout(5000);
        this.connect();
    }

    async close() {
        this.isConnected = false;
        this.client.close();
    }

    async send(queue, msg) {
        if (this.connected) {
            let channel = await this.client.createChannel();
            await channel.assertQueue(queue, {
                durable: true
            });
            await channel.sendToQueue(queue, Buffer.from(msg), {
                persistent: true
            });
            return true;
        }
        return false;
    }

    async receiving(queue, cb) {
        if (this.connected) {
            let channel = await this.client.createChannel();
            await channel.assertQueue(queue, {
                durable: true
            });
            await channel.consume(queue, cb, {
                noAck: true
            });
        } else {
            await timeout(5000);
            return receiving(queue, cb);
        }
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