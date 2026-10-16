import { Injectable, OnModuleInit, OnModuleDestroy, HttpException, HttpStatus } from '@nestjs/common';
import amqp, { Channel, ChannelModel } from 'amqplib';

@Injectable()
export class RabbitAdminService implements OnModuleInit, OnModuleDestroy {
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;
  private mgmtUrl = process.env.RABBITMQ_MGMT_URL || 'http://localhost:15672';
  private mgmtUser = process.env.RABBITMQ_USER || 'vidalstore_admin';
  private mgmtPass = process.env.RABBITMQ_PASS || 'vidalstore_pass_2026';
  private amqpUrl = process.env.RABBITMQ_URL || 'amqp://vidalstore_admin:vidalstore_pass_2026@localhost:5672';

  async onModuleInit() {
    await this.initConnection();
  }

  async onModuleDestroy() {
    try {
      if (this.channel) await this.channel.close();
      if (this.connection) await this.connection.close();
    } catch {}
  }

  private async initConnection() {
    try {
      this.connection = await amqp.connect(this.amqpUrl);
      this.channel = await this.connection.createChannel();
    } catch {
      this.connection = null;
      this.channel = null;
    }
  }

  private async getChannel(): Promise<Channel | null> {
    if (this.channel) {
      return this.channel;
    }
    await this.initConnection();
    return this.channel;
  }

  private getAuthHeader(): string {
    return 'Basic ' + Buffer.from(`${this.mgmtUser}:${this.mgmtPass}`).toString('base64');
  }

  async listarColas(): Promise<any[]> {
    try {
      const res = await fetch(`${this.mgmtUrl}/api/queues`, {
        headers: { Authorization: this.getAuthHeader() }
      });
      if (!res.ok) {
        throw new HttpException('Error al consultar colas en RabbitMQ', HttpStatus.BAD_GATEWAY);
      }
      const data = await res.json();
      return data.map((q: any) => ({
        nombre: q.name,
        mensajes: q.messages ?? 0,
        consumidores: q.consumers ?? 0,
        durable: q.durable ?? false
      }));
    } catch (err: any) {
      if (err instanceof HttpException) throw err;
      throw new HttpException('No se pudo conectar a la API de RabbitMQ', HttpStatus.BAD_GATEWAY);
    }
  }

  async crearCola(nombre: string, durable = true, argumentos: Record<string, any> = {}): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.assertQueue(nombre, { durable, arguments: argumentos });
        return { mensaje: `Cola ${nombre} creada exitosamente` };
      } catch (err: any) {
        throw new HttpException(`Error al crear cola ${nombre}: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/queues/%2F/${encodeURIComponent(nombre)}`, {
      method: 'PUT',
      headers: {
        Authorization: this.getAuthHeader(),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ durable, auto_delete: false, arguments: argumentos })
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al crear cola ${nombre}: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Cola ${nombre} creada exitosamente` };
  }

  async eliminarCola(nombre: string): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.deleteQueue(nombre);
        return { mensaje: `Cola ${nombre} eliminada exitosamente` };
      } catch (err: any) {
        throw new HttpException(`Error al eliminar cola ${nombre}: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/queues/%2F/${encodeURIComponent(nombre)}`, {
      method: 'DELETE',
      headers: { Authorization: this.getAuthHeader() }
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al eliminar cola ${nombre}: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Cola ${nombre} eliminada exitosamente` };
  }

  async listarExchanges(): Promise<any[]> {
    try {
      const res = await fetch(`${this.mgmtUrl}/api/exchanges`, {
        headers: { Authorization: this.getAuthHeader() }
      });
      if (!res.ok) {
        throw new HttpException('Error al consultar exchanges en RabbitMQ', HttpStatus.BAD_GATEWAY);
      }
      const data = await res.json();
      return data.map((e: any) => ({
        nombre: e.name,
        tipo: e.type,
        durable: e.durable ?? false
      }));
    } catch (err: any) {
      if (err instanceof HttpException) throw err;
      throw new HttpException('No se pudo conectar a la API de RabbitMQ', HttpStatus.BAD_GATEWAY);
    }
  }

  async crearExchange(nombre: string, tipo: string, durable = true): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.assertExchange(nombre, tipo, { durable });
        return { mensaje: `Exchange ${nombre} creado exitosamente` };
      } catch (err: any) {
        throw new HttpException(`Error al crear exchange ${nombre}: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/exchanges/%2F/${encodeURIComponent(nombre)}`, {
      method: 'PUT',
      headers: {
        Authorization: this.getAuthHeader(),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ type: tipo, durable, auto_delete: false })
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al crear exchange ${nombre}: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Exchange ${nombre} creado exitosamente` };
  }

  async eliminarExchange(nombre: string): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.deleteExchange(nombre);
        return { mensaje: `Exchange ${nombre} eliminado exitosamente` };
      } catch (err: any) {
        throw new HttpException(`Error al eliminar exchange ${nombre}: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/exchanges/%2F/${encodeURIComponent(nombre)}`, {
      method: 'DELETE',
      headers: { Authorization: this.getAuthHeader() }
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al eliminar exchange ${nombre}: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Exchange ${nombre} eliminado exitosamente` };
  }

  async crearBinding(exchange: string, cola: string, routingKey: string): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.bindQueue(cola, exchange, routingKey);
        return { mensaje: `Binding creado entre ${exchange} y ${cola} con clave ${routingKey}` };
      } catch (err: any) {
        throw new HttpException(`Error al crear binding: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/bindings/%2F/e/${encodeURIComponent(exchange)}/q/${encodeURIComponent(cola)}`, {
      method: 'POST',
      headers: {
        Authorization: this.getAuthHeader(),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ routing_key: routingKey })
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al crear binding: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Binding creado entre ${exchange} y ${cola} con clave ${routingKey}` };
  }

  async eliminarBinding(exchange: string, cola: string, routingKey: string): Promise<any> {
    const ch = await this.getChannel();
    if (ch) {
      try {
        await ch.unbindQueue(cola, exchange, routingKey);
        return { mensaje: `Binding eliminado entre ${exchange} y ${cola} con clave ${routingKey}` };
      } catch (err: any) {
        throw new HttpException(`Error al eliminar binding: ${err.message}`, HttpStatus.BAD_REQUEST);
      }
    }

    const res = await fetch(`${this.mgmtUrl}/api/bindings/%2F/e/${encodeURIComponent(exchange)}/q/${encodeURIComponent(cola)}/${encodeURIComponent(routingKey)}`, {
      method: 'DELETE',
      headers: { Authorization: this.getAuthHeader() }
    });

    if (!res.ok) {
      const err = await res.text();
      throw new HttpException(`Error al eliminar binding: ${err}`, HttpStatus.BAD_REQUEST);
    }

    return { mensaje: `Binding eliminado entre ${exchange} y ${cola} con clave ${routingKey}` };
  }
}
