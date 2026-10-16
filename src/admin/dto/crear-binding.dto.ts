import { IsString, IsNotEmpty } from 'class-validator';

export class CrearBindingDto {
  @IsString()
  @IsNotEmpty()
  exchange: string;

  @IsString()
  @IsNotEmpty()
  cola: string;

  @IsString()
  routingKey: string;
}
