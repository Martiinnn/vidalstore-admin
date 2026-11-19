import { IsIn, IsISO8601, IsOptional, IsString, Matches, ValidateIf } from 'class-validator';
import { MENSAJE_NOMBRE_TOPIC, NOMBRE_TOPIC } from './crear-topic.dto';

export const DESTINOS = ['principio', 'final', 'fecha'] as const;
export type Destino = (typeof DESTINOS)[number];

export class RebobinarDto {
  @IsIn(DESTINOS, { message: `hacia tiene que ser uno de: ${DESTINOS.join(', ')}` })
  hacia!: Destino;

  @ValidateIf((dto: RebobinarDto) => dto.hacia === 'fecha')
  @IsISO8601({ strict: true }, { message: 'con hacia fecha, fecha es obligatoria y va en formato ISO 8601' })
  fecha?: string;

  @IsOptional()
  @IsString({ message: 'topic tiene que ser texto' })
  @Matches(NOMBRE_TOPIC, { message: MENSAJE_NOMBRE_TOPIC })
  topic?: string;
}
