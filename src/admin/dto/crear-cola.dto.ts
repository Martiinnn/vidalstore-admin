import { IsString, IsNotEmpty, IsOptional, IsBoolean, IsObject } from 'class-validator';

export class CrearColaDto {
  @IsString()
  @IsNotEmpty()
  nombre: string;

  @IsOptional()
  @IsBoolean()
  durable?: boolean;

  @IsOptional()
  @IsObject()
  argumentos?: Record<string, any>;
}
