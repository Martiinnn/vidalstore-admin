import {
  BadRequestException,
  ConflictException,
  HttpException,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  ServiceUnavailableException,
} from '@nestjs/common';
import { setTimeout as esperar } from 'node:timers/promises';
import {
  Admin,
  ConfigResourceTypes,
  ConfigSources,
  IncrementalAlterConfigOperationTypes,
  type ClusterTopicMetadata,
} from '@platformatic/kafka';
import type { ConfigsTopicDto } from './dto/configs-topic.dto';
import { MENSAJE_NOMBRE_TOPIC, NOMBRE_TOPIC, type CrearTopicDto } from './dto/crear-topic.dto';
import type { RebobinarDto } from './dto/rebobinar.dto';

const PREFIJO_PROTEGIDO = 'vidalstore.';
const NOMBRE_GRUPO = /^[a-zA-Z0-9._-]{1,255}$/;
const PRIMERO = -2n;
const ULTIMO = -1n;

export type Topic = {
  nombre: string;
  particiones: number;
  replicas: number;
  configs: Record<string, string | null>;
  detalle: { particion: number; lider: number; replicas: number[]; isr: number[] }[];
};

type ParticionLag = { particion: number; confirmado: number | null; final: number; lag: number | null };

@Injectable()
export class KafkaAdminService implements OnModuleDestroy {
  private readonly log = new Logger(KafkaAdminService.name);
  private readonly admin: Admin;

  constructor() {
    const brokersEnv = process.env.KAFKA_BROKERS || 'localhost:9092';
    this.admin = new Admin({
      clientId: 'vidalstore-admin',
      bootstrapBrokers: brokersEnv.split(','),
      retries: 2,
      timeout: 5000,
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.admin.close();
  }

  async listarTopics(): Promise<Topic[]> {
    return this.conKafka(async () => {
      const nombres = (await this.admin.listTopics()).sort();
      return this.describir(nombres);
    });
  }

  async crearTopic(dto: CrearTopicDto): Promise<Topic> {
    this.validarNombreTopic(dto.nombre);
    return this.conKafka(async () => {
      await this.admin.createTopics({
        topics: [dto.nombre],
        partitions: dto.particiones,
        replicas: dto.replicas,
        configs: Object.entries(dto.configs ?? {})
          .filter(([, valor]) => valor !== undefined)
          .map(([name, value]) => ({ name, value: String(value) })),
      });
      this.log.log(`topic creado: ${dto.nombre} (${dto.particiones} particiones, ${dto.replicas} replicas)`);
      for (let intento = 0; intento < 20 && !(await this.admin.listTopics()).includes(dto.nombre); intento++) {
        await esperar(250);
      }
      return (await this.describir([dto.nombre]))[0];
    });
  }

  async cambiarConfig(nombre: string, dto: ConfigsTopicDto): Promise<Topic> {
    this.validarNombreTopic(nombre);
    const cambios = Object.entries(dto).filter(([, valor]) => valor !== undefined);
    if (cambios.length === 0) {
      throw new BadRequestException('manda al menos una config valida');
    }
    return this.conKafka(async () => {
      await this.metadataDe(nombre);
      await this.admin.incrementalAlterConfigs({
        resources: [
          {
            resourceType: ConfigResourceTypes.TOPIC,
            resourceName: nombre,
            configs: cambios.map(([name, value]) => ({
              name,
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: String(value),
            })),
          },
        ],
      });
      this.log.log(`configs de ${nombre}: ${cambios.map(([n, v]) => `${n}=${v}`).join(', ')}`);
      return (await this.describir([nombre]))[0];
    });
  }

  async aumentarParticiones(nombre: string, particiones: number): Promise<Topic & { aviso: string }> {
    this.validarNombreTopic(nombre);
    return this.conKafka(async () => {
      const actuales = (await this.metadataDe(nombre)).partitionsCount;
      if (particiones < actuales) {
        throw new BadRequestException(
          `${nombre} tiene ${actuales} particiones y se pidieron ${particiones}: Kafka no permite disminuirlas. ` +
            'La particion de un mensaje es el hash de su key modulo el numero de particiones; con menos, las keys ' +
            'caerian en otra particion, los eventos de un mismo juego o licencia quedarian repartidos y se romperia ' +
            'el orden secuencial, y los mensajes de las particiones quitadas no tendrian donde quedar.',
        );
      }
      if (particiones === actuales) {
        throw new BadRequestException(`${nombre} ya tiene ${actuales} particiones: pide un numero mayor`);
      }
      await this.admin.createPartitions({ topics: [{ name: nombre, count: particiones }] });
      this.log.warn(`particiones de ${nombre}: ${actuales} -> ${particiones}`);
      return {
        ...(await this.describir([nombre]))[0],
        aviso:
          `de ${actuales} a ${particiones} particiones. Los mensajes existentes conservan su particion; las nuevas ` +
          'claves podran ser asignadas a las particiones adicionales manteniendo balance horizontal.',
      };
    });
  }

  async borrarTopic(nombre: string, confirmar?: string): Promise<{ borrado: string; particiones: number }> {
    this.validarNombreTopic(nombre);
    if (nombre.startsWith(PREFIJO_PROTEGIDO) && confirmar !== nombre) {
      throw new BadRequestException(
        `${nombre} es un topic critico de VidalStore: borrarlo elimina sus mensajes y desconecta a los ` +
          `consumidores activos. Si confirmas la operacion, incluye el parametro: ?confirmar=${nombre}`,
      );
    }
    return this.conKafka(async () => {
      const particiones = (await this.metadataDe(nombre)).partitionsCount;
      await this.admin.deleteTopics({ topics: [nombre] });
      this.log.warn(`topic borrado: ${nombre}`);
      return { borrado: nombre, particiones };
    });
  }

  async listarGrupos() {
    return this.conKafka(async () => {
      const ids = [...(await this.admin.listGroups()).keys()].sort();
      if (ids.length === 0) return [];
      const descritos = await this.admin.describeGroups({ groups: ids });
      const confirmados = await this.admin.listConsumerGroupOffsets({ groups: ids });

      const topics = new Set(confirmados.flatMap((g) => g.topics.map((t) => t.name)));
      const finales = await this.finales([...topics]);

      return ids.map((id) => {
        const grupo = descritos.get(id);
        const offsets = confirmados.find((g) => g.groupId === id)?.topics ?? [];
        const porTopic = offsets
          .map((t) => {
            const particiones: ParticionLag[] = t.partitions
              .map((p) => {
                const final = finales.get(`${t.name}:${p.partitionIndex}`) ?? 0;
                const confirmado = p.committedOffset < 0n ? null : Number(p.committedOffset);
                return { particion: p.partitionIndex, confirmado, final, lag: confirmado === null ? null : final - confirmado };
              })
              .sort((a, b) => a.particion - b.particion);
            return { topic: t.name, lag: particiones.reduce((s, p) => s + (p.lag ?? 0), 0), particiones };
          })
          .sort((a, b) => a.topic.localeCompare(b.topic));
        return {
          grupo: id,
          estado: grupo?.state ?? 'desconocido',
          miembros: grupo?.members.size ?? 0,
          lagTotal: porTopic.reduce((s, t) => s + t.lag, 0),
          topics: porTopic,
        };
      });
    });
  }

  async rebobinar(id: string, dto: RebobinarDto) {
    if (!NOMBRE_GRUPO.test(id)) throw new BadRequestException('el id de un grupo solo admite caracteres validos');
    return this.conKafka(async () => {
      if (!(await this.admin.listGroups()).has(id)) throw new NotFoundException(`no existe el consumer group ${id}`);
      const grupo = (await this.admin.describeGroups({ groups: [id] })).get(id);
      const miembros = grupo?.members.size ?? 0;
      if (miembros > 0) {
        throw new ConflictException(
          `${id} tiene ${miembros} consumidor(es) conectado(s) (estado ${grupo?.state}). Kafka solo permite cambiar ` +
            'offsets de un grupo sin miembros activos: deten el servicio vidalstore-eventos antes de solicitar el rebobinado.',
        );
      }

      const confirmados = (await this.admin.listConsumerGroupOffsets({ groups: [id] }))[0]?.topics ?? [];
      const topics = dto.topic ? [dto.topic] : confirmados.map((t) => t.name);
      if (topics.length === 0) {
        throw new BadRequestException(`${id} no tiene offsets confirmados en ningun topic: indica cual con "topic"`);
      }
      const antes = new Map(
        confirmados.flatMap((t) => t.partitions.map((p) => [`${t.name}:${p.partitionIndex}`, Number(p.committedOffset)])),
      );

      const destino = dto.hacia === 'principio' ? PRIMERO : dto.hacia === 'final' ? ULTIMO : BigInt(Date.parse(dto.fecha!));
      const nuevos = await this.offsetsEn(topics, destino);
      const finales = dto.hacia === 'fecha' ? await this.finales(topics) : new Map<string, number>();
      const resultado = topics.map((topic) => ({
        topic,
        particiones: [...nuevos.entries()]
          .filter(([clave]) => clave.startsWith(`${topic}:`))
          .map(([clave, offset]) => {
            const ahora = offset < 0 ? (finales.get(clave) ?? 0) : offset;
            return { particion: Number(clave.slice(topic.length + 1)), antes: antes.get(clave) ?? null, ahora };
          })
          .sort((a, b) => a.particion - b.particion),
      }));

      await this.admin.alterConsumerGroupOffsets({
        groupId: id,
        topics: resultado.map((t) => ({
          name: t.topic,
          partitionOffsets: t.particiones.map((p) => ({ partition: p.particion, offset: BigInt(p.ahora) })),
        })),
      });
      this.log.warn(`grupo ${id} movido hacia ${dto.hacia}${dto.fecha ? ` ${dto.fecha}` : ''}: ${topics.join(', ')}`);
      return { grupo: id, hacia: dto.hacia, ...(dto.fecha ? { fecha: dto.fecha } : {}), topics: resultado };
    });
  }

  private validarNombreTopic(nombre: string): void {
    if (!NOMBRE_TOPIC.test(nombre)) throw new BadRequestException(MENSAJE_NOMBRE_TOPIC);
    if (nombre.startsWith('__')) {
      throw new BadRequestException(`${nombre} es un topic interno de Kafka`);
    }
  }

  private async metadata(topics: string[]): Promise<Map<string, ClusterTopicMetadata>> {
    const existentes = new Set(await this.admin.listTopics());
    const falta = topics.find((t) => !existentes.has(t));
    if (falta) throw new NotFoundException(`no existe el topic ${falta}`);
    return (await this.admin.metadata({ topics, forceUpdate: true })).topics;
  }

  private async metadataDe(nombre: string): Promise<ClusterTopicMetadata> {
    return (await this.metadata([nombre])).get(nombre)!;
  }

  private async describir(nombres: string[]): Promise<Topic[]> {
    if (nombres.length === 0) return [];
    const metadata = await this.metadata(nombres);
    const configs = await this.admin.describeConfigs({
      resources: nombres.map((n) => ({ resourceType: ConfigResourceTypes.TOPIC, resourceName: n })),
    });
    return nombres.map((nombre) => {
      const t = metadata.get(nombre)!;
      const propias = configs
        .find((c) => c.resourceName === nombre)
        ?.configs.filter((c) => c.configSource === ConfigSources.DYNAMIC_TOPIC_CONFIG)
        .sort((a, b) => a.name.localeCompare(b.name));
      return {
        nombre,
        particiones: t.partitionsCount,
        replicas: t.partitions[0]?.replicas.length ?? 0,
        configs: Object.fromEntries((propias ?? []).map((c) => [c.name, c.value ?? null])),
        detalle: t.partitions.map((p, particion) => ({ particion, lider: p.leader, replicas: p.replicas, isr: p.isr })),
      };
    });
  }

  private async offsetsEn(topics: string[], timestamp: bigint): Promise<Map<string, number>> {
    const resultado = new Map<string, number>();
    if (topics.length === 0) return resultado;
    const metadata = await this.metadata(topics);
    const pedido = topics.map((name) => {
      const t = metadata.get(name)!;
      return { name, partitions: t.partitions.map((_, partitionIndex) => ({ partitionIndex, timestamp })) };
    });
    for (const t of await this.admin.listOffsets({ topics: pedido })) {
      for (const p of t.partitions) resultado.set(`${t.name}:${p.partitionIndex}`, Number(p.offset));
    }
    return resultado;
  }

  private finales(topics: string[]): Promise<Map<string, number>> {
    return this.offsetsEn(topics, ULTIMO);
  }

  private async conKafka<T>(operacion: () => Promise<T>): Promise<T> {
    try {
      return await operacion();
    } catch (e) {
      throw this.traducir(e);
    }
  }

  private traducir(e: unknown): Error {
    if (e instanceof HttpException) return e;
    const protocolo = errorDeProtocolo(e);
    if (protocolo) {
      const detalle = protocolo.serverErrorMessage ? `${protocolo.apiId}: ${protocolo.serverErrorMessage}` : protocolo.apiId;
      switch (protocolo.apiId) {
        case 'TOPIC_ALREADY_EXISTS':
          return new ConflictException(`ya existe: ${detalle}`);
        case 'UNKNOWN_TOPIC_OR_PARTITION':
        case 'UNKNOWN_TOPIC_ID':
        case 'GROUP_ID_NOT_FOUND':
          return new NotFoundException(detalle);
        case 'NON_EMPTY_GROUP':
        case 'UNKNOWN_MEMBER_ID':
        case 'REBALANCE_IN_PROGRESS':
          return new ConflictException(`el grupo tiene consumidores conectados: ${detalle}`);
        case 'INVALID_PARTITIONS':
        case 'INVALID_REPLICATION_FACTOR':
        case 'INVALID_REPLICA_ASSIGNMENT':
        case 'INVALID_CONFIG':
        case 'INVALID_TOPIC_EXCEPTION':
        case 'POLICY_VIOLATION':
        case 'INVALID_REQUEST':
          return new BadRequestException(`Kafka rechazo el pedido: ${detalle}`);
      }
      return new ServiceUnavailableException(`Kafka respondio con un error: ${detalle}`);
    }
    const mensaje = e instanceof Error ? e.message : String(e);
    this.log.error(`Kafka no responde: ${mensaje}`);
    return new ServiceUnavailableException(`Kafka no responde: ${mensaje}`);
  }
}

type ErrorDeProtocolo = { apiId: string; serverErrorMessage?: string | null };

function errorDeProtocolo(e: unknown, profundidad = 0): ErrorDeProtocolo | undefined {
  if (!e || typeof e !== 'object' || profundidad > 5) return undefined;
  if ('apiId' in e && typeof e.apiId === 'string') return e as ErrorDeProtocolo;
  const hijos = [...('errors' in e && Array.isArray(e.errors) ? e.errors : []), ...('cause' in e ? [e.cause] : [])];
  for (const hijo of hijos) {
    const encontrado = errorDeProtocolo(hijo, profundidad + 1);
    if (encontrado) return encontrado;
  }
  return undefined;
}
