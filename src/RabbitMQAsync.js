import amqp from 'amqplib';
import { timeout } from '@azteam/ultilities';


class RabbitMQAsync {
    constructor(config) {
        this.connected = false;
        this.config = config;
        this.connect();
        this.channel = null;
    }

    async waitConnection(n = 10) {
        let i = 0;
        while (!this.connected) {
            ++i;
            if (i >= n) {
                return false;
            }
            await timeout(1000);
        }
        return true;
    }

    async connect() {
        this._alert('connecting', 'MQ connecting...');
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        try {
            this.client = await amqp.connect(`amqp://${this.config.host}`, opt);
            this.connected = true;
            this.channel = this.client.createChannel();

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
        this.connected = false;
        this.client.close();
    }

    async send(queue, msg = {}) {
        if (this.connected && this.channel) {
            try {
                this.channel.assertQueue(queue, {
                    durable: true,
                });
                this.channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)), {
                    persistent: true
                });
                return true;

            } catch (err) {
                await timeout(5000);
                return this.send(queue, msg);
            }
        } else {
            await timeout(5000);
            return this.send(queue, msg);
        }
        return false;
    }

    async receiving(queue, cb, callbackError = null) {
        if (this.connected) {

            try {
                this.channel.assertQueue(queue, {
                    durable: true
                });
                this.channel.prefetch(1);
                this.channel.consume(queue, async function(msg) {
                    try {
                        const data = JSON.parse(msg.content.toString());
                        await cb(data);
                        this.channel.ack(msg);
                    } catch (err) {
                        this.channel.nack(msg);
                        throw err;
                    }
                });
            } catch (err) {
                callbackError && callbackError(err);
                await timeout(5000);
                return this.receiving(queue, cb, callbackError);
            }


        } else {
            await timeout(5000);
            return this.receiving(queue, cb, callbackError);
        }
    }

    setAlertCallback(callback) {
        this.alertCallback = callback;
    }

    _alert(status, msg) {
        if (typeof this.alertCallback === 'function') {
            this.alertCallback(status, msg);
        } else {
            console.log(status, msg);
        }
    }
}

export default RabbitMQAsync;