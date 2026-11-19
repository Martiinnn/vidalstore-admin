import { Body, Controller, Delete, Get, HttpCode, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { CognitoAuthGuard } from '../auth/cognito-auth.guard';
import { RolesGuard } from '../auth/roles.guard';
import { Roles } from '../auth/roles.decorator';
import { ConfigsTopicDto } from './dto/configs-topic.dto';
import { CrearTopicDto } from './dto/crear-topic.dto';
import { AumentarParticionesDto } from './dto/aumentar-particiones.dto';
import { RebobinarDto } from './dto/rebobinar.dto';
import { KafkaAdminService, type Topic } from './kafka-admin.service';

@Controller('v1/admin/kafka')
@UseGuards(CognitoAuthGuard, RolesGuard)
@Roles('administradores')
export class KafkaAdminController {
  constructor(private readonly servicio: KafkaAdminService) {}

  @Get('topics')
  listarTopics(): Promise<Topic[]> {
    return this.servicio.listarTopics();
  }

  @Post('topics')
  @HttpCode(201)
  crearTopic(@Body() dto: CrearTopicDto): Promise<Topic> {
    return this.servicio.crearTopic(dto);
  }

  @Patch('topics/:nombre/config')
  cambiarConfig(@Param('nombre') nombre: string, @Body() dto: ConfigsTopicDto): Promise<Topic> {
    return this.servicio.cambiarConfig(nombre, dto);
  }

  @Post('topics/:nombre/particiones')
  aumentarParticiones(@Param('nombre') nombre: string, @Body() dto: AumentarParticionesDto): Promise<Topic & { aviso: string }> {
    return this.servicio.aumentarParticiones(nombre, dto.particiones);
  }

  @Delete('topics/:nombre')
  borrarTopic(@Param('nombre') nombre: string, @Query('confirmar') confirmar?: string) {
    return this.servicio.borrarTopic(nombre, confirmar);
  }

  @Get('grupos')
  listarGrupos() {
    return this.servicio.listarGrupos();
  }

  @Post('grupos/:id/offsets')
  rebobinar(@Param('id') id: string, @Body() dto: RebobinarDto) {
    return this.servicio.rebobinar(id, dto);
  }
}
