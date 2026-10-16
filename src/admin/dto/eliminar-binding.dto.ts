import { IsString, IsNotEmpty } from 'class-validator';

export class EliminarBindingDto {
  @IsString()
  @IsNotEmpty()
  exchange: string;

  @IsString()
  @IsNotEmpty()
  cola: string;

  @IsString()
  routingKey: string;
}
