import { IsIn, IsOptional, Matches } from 'class-validator';

const ENTERO = /^\d+$/;
const ENTERO_O_MENOS_UNO = /^(-1|\d+)$/;

export const POLITICAS = ['delete', 'compact', 'compact,delete', 'delete,compact'] as const;

export class ConfigsTopicDto {
  @IsOptional()
  @Matches(ENTERO_O_MENOS_UNO, {
    message: 'retention.ms va como texto, en milisegundos, o "-1" para no borrar por tiempo',
  })
  'retention.ms'?: string;

  @IsOptional()
  @IsIn(POLITICAS, { message: `cleanup.policy tiene que ser una de: ${POLITICAS.join(' | ')}` })
  'cleanup.policy'?: string;

  @IsOptional()
  @IsIn(['1', '2', '3'], {
    message: 'min.insync.replicas va como texto y entre "1" y "3"',
  })
  'min.insync.replicas'?: string;

  @IsOptional()
  @Matches(ENTERO_O_MENOS_UNO, { message: 'retention.bytes va como texto, en bytes por particion, o "-1" para no limitar' })
  'retention.bytes'?: string;

  @IsOptional()
  @Matches(ENTERO, { message: 'segment.ms va como texto, en milisegundos' })
  'segment.ms'?: string;

  @IsOptional()
  @Matches(ENTERO, { message: 'max.message.bytes va como texto, en bytes' })
  'max.message.bytes'?: string;
}
