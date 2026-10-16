import { IsString, IsNotEmpty, IsIn, IsOptional, IsBoolean } from 'class-validator';

export class CrearExchangeDto {
  @IsString()
  @IsNotEmpty()
  nombre: string;

  @IsString()
  @IsIn(['direct', 'topic', 'fanout', 'headers'])
  tipo: string;

  @IsOptional()
  @IsBoolean()
  durable?: boolean;
}
