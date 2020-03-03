import RabbitMQAsync from './RabbitMQAsync';
import {timeout} from '@azteam/ultilities';

class Provider {
    constructor(configs) {
        this.configs = configs;
        this.connections = {};
    }
    async getConnection(name) {
        if (!this.connections[name]) {
            const rabbitmq = new RabbitMQAsync(this.configs[name]);

            while (!rabbitmq.connected) {
                await timeout(1000);
            }
            this.connections[name] = rabbitmq;
        }
        return this.connections[name];
    }

}

export default Provider;